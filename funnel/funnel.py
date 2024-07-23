from pathlib import Path
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any
from datetime import datetime, timezone
import json
import shutil
import re
import textwrap
from tempfile import NamedTemporaryFile
import subprocess
import time
from typing import List
from argparse import ArgumentParser
import traceback
from collections import defaultdict
import math
import sys
import inspect

from funnel.utils import item_ids_in_dir
from funnel.utils import dumps


class Reject(Exception):
    """
    Raised when a step rejects (filters out) the current item.
    """

    def __init__(self, reason):
        self.reason = reason


# return this from Step#item to indicte that the item is unchanged from the
# previous step. Used both for convenience and to reduce disk storage (we symlink
# to the previous item instead of copying).
COPY = object()

# technically 1001, but let's not push our luck.
# This is also configurable in slurm, but I don't feel like parsing slurm conf
# files right now. 1001 is the default and also the value used on discovery.
SLURM_MAX_ITEMS = 1000

# maximum number of jobs a user can have submitted/queued/running/etc simultaneously.
# I've experimentally determined MaxSubmitJobPerUserLimit to be 1500 via
# the following script and bisecting the value of n which lets two batches be submitted.
# inflection point is at n=749/750, so 2n = 1500.
#
#   #!/bin/bash
#   #SBATCH --partition=express
#   #SBATCH --nodes 1
#   #SBATCH --ntasks 1
#   #SBATCH --array=0-1000
#
#   sleep 5
#
# This limit is equivalent to MaxSubmitPU in sacctmgr show qos | grep normal,
# even though the rest of the partitions don't have a MaxSubmitPU set. possibly
# partitions take default settings from "normal" if not set. unsure.
SLURM_MAX_SUBMITTED_JOBS = 1500

# some partitions have a different max time than the default. we'll always use
# the max, unless the Step specifies otherwise.
SLURM_PARTITION_MAX_TIMES = {
    # hh:mm:ss
    "debug": "00:20:00",
    "express": "00:60:00",
    "short": "24:00:00",
}


@dataclass
class Run:
    started_at: datetime
    ended_at: datetime
    items: List[Any]


class Step:
    ## required config options
    name = None
    output = None

    ## other config options
    items_per_node = 1
    # call self.item for each item this many times, passing iteration=n on the
    # nth iteration.
    iterations = 1

    ## slurm/discovery config options
    partition = "short"
    # set to None to use default max time limit for this partition
    time_limit = None
    cpus_per_task = 1
    # memory per task in mb
    memory = None

    def __init__(self, storage_dir, *, parent):
        # step-specific metadata. steps can put anything they want here, e.g.
        # stacktraces for items that were rejected as a result of errors.
        self.metadata = {}
        self.storage_dir = storage_dir / self.name
        self.metadata_dir = self.storage_dir / "_processing_results"
        self.parent = parent

    def __str__(self):
        return self.name

    __repr__ = __str__

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    @abstractmethod
    def item(self, item, i):
        pass

    def output_path(self, i) -> Path:
        return self.storage_dir / f"{i}"

    def add_metadata(self, metadata):
        self.metadata = {**metadata, **self.metadata}

    def _write_metadata(self, metadata, i):
        metadata = {"metadata": self.metadata, **metadata}
        self.metadata_dir.mkdir(exist_ok=True)
        with open(self.metadata_dir / f"{i}", "w+") as f:
            f.write(dumps(metadata) + "\n")

    def process_item(self, item, i, *, iteration):
        # clear on metadata each item, so any add_metadata is fresh
        self.metadata.clear()

        # only pass iteration if self.item requests it with a kw-only param
        kwargs = {}
        for param in inspect.signature(self.item).parameters.values():
            if (
                param.kind is inspect.Parameter.KEYWORD_ONLY
                and param.name == "iteration"
            ):
                kwargs["iteration"] = iteration

        try:
            output = self.item(item, i, **kwargs)
        except Reject as e:
            # if the step wrote to the output directory here before rejecting it,
            # clean it up so future steps don't think it was valid.
            p = self.output_path(i)
            if p.is_file():
                p.unlink()
            if p.is_dir():
                shutil.rmtree(p)

            metadata = {"status": "rejected", "rejected_reason": e.reason}
            self._write_metadata(metadata, i)
            return
        except Exception:
            metadata = {
                "status": "error",
                "error_message": traceback.format_exc(),
            }
            self._write_metadata(metadata, i)
            return

        output_path = self.output_path(i)
        if output is COPY:
            assert self.parent is not None
            previous_output_path = self.parent.output_path(i)
            output_path.symlink_to(previous_output_path.resolve())
        elif self.output == "json":
            if output is None:
                raise Exception(
                    'Step must return a value from item() for output == "json"'
                )

            with open(output_path, "w+") as f:
                f.write(dumps(output) + "\n")
        # for output == "path" which doesn't return COPY, the step is responsible
        # for writing to output_path itself. TODO we could check that something
        # has in fact been written to output_path in this case

        metadata = {"status": "valid"}
        self._write_metadata(metadata, i)

    def log(self, message):
        print(f"[{self.name}] {message}")


class InputStep(Step):
    def __init__(self, storage_dir):
        super().__init__(storage_dir, parent=None)
        # for caching purposes
        self._items = None

    @abstractmethod
    def items(self):
        pass

    # weird song and dance to get the correct caching behavior for items()
    def get_items(self):
        self._items = self.items()
        return self._items

    def item(self, item, i):
        if self._items is None:
            self._items = self.items()
        return self._items[i]


class FilterStep(Step):
    # the output of a filter step is the same as the output of its parent step.
    output = "copy"

    def item(self, item, i):
        if not self.filter(item, i):
            raise Reject("rejected by filter")
        # filter steps don't modify the accepted items, so tell Funnel to copy
        # the item from the previous step. This differs from `return item` as
        # the item may be a path with a full directory structure (output = "path").
        return COPY

    @abstractmethod
    def filter(self, item, i):
        """
        Returns True if the item is valid and should be kept, and False
        otherwise.
        """


class Script:
    # required config options
    name = None

    # optional config options
    # TODO enforce depends_on by running those steps if not already ran? or just
    # assert that these steps have run. need a way to check/update which steps have
    # finished. use central _meta dir for this
    depends_on = []

    def __init__(self, funnel):
        self.funnel = funnel
        self.argparser = ArgumentParser()
        self.add_arguments(self.argparser)

    def add_arguments(self, parser):
        pass

    @abstractmethod
    def run(self, **kwargs):
        pass


def _check_step_class(StepClass):
    assert StepClass.name is not None, f"must set a name for {StepClass}"
    assert StepClass.output is not None, f"must set an output for {StepClass}"


# for ergonomic return chaining
class StepAdder:
    def __init__(self, funnel, step):
        self.funnel = funnel
        self.step = step

    def add_child_step(self, StepClass):
        return self.funnel.add_step(StepClass, parent=self.step)


class Funnel:
    metadata_filename = "_statistics.json"

    def __init__(self, storage_dir):
        self.storage_dir = Path(storage_dir)
        self.meta_dir = self.storage_dir / "_meta"
        self.meta_scripts_dir = self.meta_dir / "scripts"
        self.meta_output_dir = self.meta_dir / "output"
        self.meta_errors_dir = self.meta_dir / "errors"

        # make sure all the dirs exist
        for p in [
            self.storage_dir,
            self.meta_dir,
            self.meta_scripts_dir,
            self.meta_output_dir,
            self.meta_errors_dir,
        ]:
            p.mkdir(parents=True, exist_ok=True)

        self.steps = []
        self.scripts = []
        self._initial_step = None
        # mapping of step to their parent
        self._parent_step = {}
        # mapping of step to their children
        self._step_children = defaultdict(list)

        self.argparser = ArgumentParser()
        self.argparser.add_argument(
            "--discovery-batch",
            dest="discovery_batch",
            choices=["true", "false"],
            default=None,
        )
        self.argparser.add_argument("--step", dest="step")
        self.argparser.add_argument(
            "--item-ids", dest="item_ids", nargs="+", default=None, type=int
        )
        self.argparser.add_argument("--from-step", dest="from_step")
        self.argparser.add_argument("--after-step", dest="after_step")
        self.argparser.add_argument("--to-step", dest="to_step")
        self.argparser.add_argument("--script", dest="script")

        # these should not be set by users. set internally by code.
        self.argparser.add_argument("--in-batch", dest="in_batch", action="store_true")
        # step name
        self.argparser.add_argument("--batch-step", dest="batch_step")
        # item id in that step
        self.argparser.add_argument("--batch-item", dest="batch_item", type=int)
        # step iteration for this item
        self.argparser.add_argument(
            "--batch-iteration", dest="batch_iteration", type=int
        )

    def create_temporary_script(self, script_text, *, suffix) -> NamedTemporaryFile:
        script_text = textwrap.dedent(script_text).strip()
        with NamedTemporaryFile(
            mode="w+", suffix=suffix, delete=False, dir=self.meta_scripts_dir
        ) as f:
            print(f"creating temporary script at {f.name}")
            f.write(script_text)
        return f

    def _find_step(self, step_name) -> Step:
        for step in self.all_steps():
            if step.name == step_name:
                return step
        raise Exception(
            f"could not find step {step_name} (options: {self.all_steps()})"
        )

    def _find_script(self, script_name) -> Script:
        for script in self.scripts:
            if script.name == script_name:
                return script
        raise Exception(
            f"could not find script {script_name} (options: {[script.name for script in self.scripts]})"
        )

    def initial_step(self, StepClass):
        _check_step_class(StepClass)
        step = StepClass(self.storage_dir)
        assert isinstance(step, InputStep)

        self._initial_step = step
        return StepAdder(self, step)

    def add_step(self, StepClass, *, parent):
        _check_step_class(StepClass)

        step = StepClass(self.storage_dir, parent=parent)
        # a step is an input step iff it is the first step in the funnel.
        assert not isinstance(step, InputStep)

        self._parent_step[step] = parent
        self._step_children[parent].append(step)

        return StepAdder(self, step)

    def add_script(self, ScriptClass):
        script = ScriptClass(self)
        self.scripts.append(script)

    def children_of(self, step, *, include_parent=False):
        children = []
        for child in self._step_children[step]:
            children.append(child)
            children += self.children_of(child)
        if include_parent:
            children = [step] + children
        return children

    def all_steps(self):
        return self.children_of(self._initial_step, include_parent=True)

    def run(self, argv, *, discovery_batch=False):
        """
        Parameters
        ----------
        discovery_batch: bool
            Whether to run this Funnel in batch mode on Northeastern's discovery.
            If True, a batch job will be submitted for each step, with a number
            of single jobs equal to the number of items for that step. Once all
            items in the step finish processing (i.e. the batch job finishes),
            the next step will be run in the same fashion.

            The only step that runs sequentially is the InputStep, because we
            don't know ahead of time how many items there are.

            If discovery_batch is True, you *must* be running this on discovery,
            as we will attempt to run various discovery-specific commands which
            will error if run on any other system.

            Can be overriden with the --discovery-batch command line argument.
        """
        args, remaining_args = self.argparser.parse_known_args(argv[1:])

        if args.discovery_batch is not None:
            discovery_batch = args.discovery_batch == "true"

        if args.script is not None:
            script = self._find_script(args.script)
            args = script.argparser.parse_args(remaining_args)
            script.run(**args.__dict__)
            return

        # unknown args are further parsed by --script, but otherwise disallowed.
        if remaining_args:
            self.argparser.error(f"unrecognized arguments: {' '.join(remaining_args)}")

        # we're in the middle of a batch. run a single item in a single step.
        if args.in_batch:
            step = self._find_step(args.batch_step)
            self._run_item(step, args.batch_item, for_iteration=args.batch_iteration)
            return

        if args.step is not None:
            # run exactly this step
            steps = [self._find_step(args.step)]
        elif args.to_step is not None:
            step = self._find_step(args.to_step)
            steps = [step]
            while step.parent is not None:
                steps.insert(0, step.parent)
                step = step.parent
        elif args.from_step is not None:
            # all steps after (and including) this step.
            step = self._find_step(args.from_step)
            steps = self.children_of(step, include_parent=True)
        elif args.after_step is not None:
            # like from_step, but not including the specified step.
            # this will get more complicated (and diverge further from from_step)
            # if/when we add a tree structure to steps, ie branching steps which
            # can compute final leaf-like computations in parallel with other
            # sibling steps (which then continue along their children steps).
            step = self._find_step(args.after_step)
            steps = self.children_of(step)
        else:
            steps = self.all_steps()

        for step in steps:
            if args.item_ids:
                for item_id in args.item_ids:
                    self._run_item(step, item_id)
            else:
                self._run_step(step, argv, discovery_batch=discovery_batch)

    def run_from_step(self, /, StepClass):
        """
        Runs the funnel from (and including) the given step, but not any steps
        before.
        """
        step = self._find_step(StepClass.name)
        i = self.steps.index(step)
        for step in self.steps[i:]:
            self._run_step(step)

    def run_single_step(self, /, StepClass):
        step = self._find_step(StepClass.name)
        self._run_step(step)

    def _input_type(self, step):
        parent_step = self._parent_step[step]
        # a step's input is equal to its parent step's output. If that parent
        # has an output of COPY, that means its output is the same as *its*
        # parent step (the current step's grandparent).
        input_type = parent_step.output
        if input_type == "copy":
            return self._input_type(parent_step)
        return input_type

    def _input(self, step, i):
        parent_step = self._parent_step[step]
        p = parent_step.output_path(i)
        input_type = self._input_type(step)
        if input_type == "json":
            with open(p) as f:
                val = f.read()
                return json.loads(val)
        if input_type == "path":
            return p

    def _collect_metadata(self, step: Step):
        metadata = {
            "count_valid": 0,
            "count_rejected": 0,
            "count_error": 0,
            "rejected": {},
            "errors": {},
            "item_metadata": {},
        }
        for p in step.metadata_dir.glob("*"):
            # ignore .DS_Store and other garbage
            if not re.match(r"\d+", p.name):
                continue
            i = int(p.name)
            with open(p) as f:
                item_metadata = json.loads(f.read())

            if item_metadata["status"] == "rejected":
                metadata["count_rejected"] += 1
                metadata["rejected"][i] = item_metadata["rejected_reason"]
            elif item_metadata["status"] == "error":
                metadata["count_error"] += 1
                metadata["errors"][i] = item_metadata["error_message"]
            else:
                metadata["count_valid"] += 1
            if item_metadata["metadata"]:
                metadata["item_metadata"][i] = item_metadata["metadata"]

        return metadata

    def _run_item(self, step: Step, i, *, for_iteration=None):
        val = self._input(step, i)
        iterations = (
            range(step.iterations) if for_iteration is None else [for_iteration]
        )
        for iteration in iterations:
            step.process_item(val, i, iteration=iteration)

    def _run_step(self, step: Step, argv: List[str], *, discovery_batch=False):
        print(f"running step {step.name}")
        # recreate the directory for this step
        if step.storage_dir.exists():
            shutil.rmtree(step.storage_dir)
        step.storage_dir.mkdir()

        started_at = datetime.now(timezone.utc).timestamp()

        # InputStep has to be handled a bit specially. This is ugly and should
        # be refactored so we can unify it with standard Steps. The issue is
        # it operates at a batch level of all items, instead of a per-item basis,
        # because there is no "previous item" to operate on.
        if isinstance(step, InputStep):
            # load all the items...
            items = step.get_items()
            # for now, multi-iteration input steps are nonsensical.
            assert step.iterations == 1
            # ...then (ab)use process_item to handle writing each item to its file
            # (if the InputStep has output == "json").
            for i, item in enumerate(items):
                step.process_item(item, i, iteration=0)
        else:
            parent_step = self._parent_step[step]
            item_ids = item_ids_in_dir(parent_step.storage_dir)
            item_ids = sorted(item_ids)
            if discovery_batch:
                # launch the array job for processing this step.
                caller_file = argv[0]

                # https://rc-docs.northeastern.edu/en/latest/hardware/partitions.html
                # %A = array job ID, %a = array index
                #
                # good docs for tasks vs cpus vs cores vs ...:
                # https://login.scg.stanford.edu/faqs/cores/
                # slightly worse reference:
                # https://docs.ycrc.yale.edu/clusters-at-yale/job-scheduling/

                # slurm has a hard limit on --array, so meta-batch ourselves here.
                remaining = []
                for iteration in range(step.iterations):
                    remaining += [(iteration, item_id) for item_id in item_ids]
                partition = step.partition
                time_limit = SLURM_PARTITION_MAX_TIMES[partition]
                if step.time_limit is not None:
                    time_limit = step.time_limit
                items_per_node = step.items_per_node
                max_items_per_batch = SLURM_MAX_ITEMS * items_per_node
                processes = []
                while remaining:
                    batch = remaining[:max_items_per_batch]
                    remaining = remaining[max_items_per_batch:]
                    array_str = f"0-{math.ceil(len(batch) / items_per_node) - 1}"

                    # use the same python as we were executed with
                    python = sys.executable
                    # > If Python is unable to retrieve the real path to its
                    # > executable, sys.executable will be an empty string or None
                    # > https://docs.python.org/3/library/sys.html
                    assert python not in [None, ""]
                    python_script_file = self.create_temporary_script(
                        f"""
                        import os

                        item_ids = [{", ".join([str([iteration, id]) for (iteration, id) in batch])}]
                        task_id = int(os.environ["SLURM_ARRAY_TASK_ID"])

                        for i in range({items_per_node}):
                            item_index = task_id * {items_per_node} + i

                            # can happen if items_per_node > 1, we're the last task in the array,
                            # and the number of items doesn't fit cleanly into items_per_node.
                            # e.g. consider batch=range(100), items_per_node=3. then the last task
                            # has only a single item (index 100) to process.
                            if item_index > len(item_ids):
                                print(f"index {{item_index}} out of bounds, skipping ({items_per_node=}, {{task_id=}})")
                                break

                            (iteration, item_id) = item_ids[item_index]
                            os.system(f'{python} {caller_file} --in-batch --batch-step "{step.name}" --batch-iteration {{iteration}} --batch-item {{item_id}}')
                        """,
                        suffix=".py",
                    )
                    array_job_file = self.create_temporary_script(
                        f"""
                        #!/bin/bash
                        #SBATCH --partition={partition}
                        #SBATCH --job-name {step.name}
                        #SBATCH --nodes 1
                        #SBATCH --ntasks 1
                        #SBATCH --cpus-per-task {step.cpus_per_task}
                        #SBATCH --array={array_str}
                        #SBATCH --time={time_limit}
                        {f"#SBATCH --mem={step.memory}" if step.memory is not None else ""}
                        #SBATCH -o {self.meta_output_dir}/%A_%a.txt
                        #SBATCH -e {self.meta_errors_dir}/%A_%a.txt

                        {python} {python_script_file.name}
                        """,
                        suffix=".sh",
                    )

                    while True:
                        num_submitted_jobs = subprocess.check_output(
                            ["squeue", "--me", "--noheader", "--array"], text=True
                        )
                        num_submitted_jobs = len(num_submitted_jobs.split("\n")) - 1
                        # avoid launching the next batch until doing so would keep us under
                        # SLURM_MAX_SUBMITTED_JOBS, plus some small leeway.
                        if (
                            len(batch) + num_submitted_jobs
                            < SLURM_MAX_SUBMITTED_JOBS - 5
                        ):
                            break
                        time.sleep(60)

                    process = subprocess.Popen(
                        ["sbatch", "--wait", array_job_file.name]
                    )
                    processes.append(process)

                for process in processes:
                    process.wait()

                # even though we waited for all processes, let's give it a tiny
                # bit longer to clean up, just in case.
                time.sleep(1)
            else:
                # no discovery batch mode. execute normally (sequential execution).
                for item_id in item_ids:
                    self._run_item(step, item_id)

        ended_at = datetime.now(timezone.utc).timestamp()
        metadata = self._collect_metadata(step)
        metadata = {"started_at": started_at, "ended_at": ended_at, **metadata}
        with open(step.storage_dir / self.metadata_filename, "w+") as f:
            f.write(dumps(metadata) + "\n")

        # now that we're done collecting metadata, we can remove that directory.
        # this avoids confusing when looking at the data; the intermediate step
        # is only necessary to accomodate the distributed nature of discovery.
        # this line can be commented out if needed for debugging.
        #
        # it's possible the dir may not exist if the step had 0 input items
        # (no items processed).
        if step.metadata_dir.exists():
            shutil.rmtree(step.metadata_dir)
