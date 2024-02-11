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
import getpass

from funnel.utils import highest_numbered_file


class Reject(Exception):
    """
    Raised when a step rejects (filters out) the current item.
    """


# return this from Step#item to indicte that the item is unchanged from the
# previous step. Used both for convenience and to reduce disk storage (we symlink
# to the previous item instead of copying).
COPY = object()


@dataclass
class Run:
    started_at: datetime
    ended_at: datetime
    items: list[Any]


class Step:
    # subclasses must set these at the class level
    name = None
    output = None

    def __init__(self, storage_dir, *, parent_step):
        # step-specific metadata. steps can put anything they want here, e.g.
        # stacktraces for items that were rejected as a result of errors.
        self.metadata = {}
        self.storage_dir = storage_dir / self.name
        self.metadata_dir = self.storage_dir / "_processing_results"
        self.parent_step = parent_step
        # use this instead of i when writing filenames, because filtering out
        # elements will break up the ordering (e.g. [0, 2, 4, 5])
        self.valid_items = 0

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

    def output_path(self, i):
        return self.storage_dir / f"{i}"

    def add_metadata(self, metadata):
        self.metadata |= metadata

    def _write_metadata(self, metadata, i):
        metadata = metadata | {"metadata": self.metadata}
        self.metadata_dir.mkdir(exist_ok=True)
        with open(self.metadata_dir / f"{i}", "w+") as f:
            v = json.dumps(metadata)
            f.write(v)

    def process_item(self, item, i):
        # clear metadata each item, so any add_metadata is fresh
        self.metadata.clear()

        try:
            output = self.item(item, i)
        except Reject:
            metadata = {"status": "rejected"}
            self._write_metadata(metadata, i)
            return

        # use valid_items as it ensures sequentially ordering when filtering,
        # whereas i does not (i is the previous step's i)
        output_path = self.output_path(self.valid_items)

        if output is COPY:
            assert self.parent_step is not None
            previous_output_path = self.parent_step.output_path(i)
            output_path.symlink_to(previous_output_path)
        elif self.output == "object":
            if output is None:
                raise Exception(
                    'Step must return a value from item() for output == "object"'
                )

            output = json.dumps(output)

            with open(output_path, "w+") as f:
                f.write(output)
        # for output == "path" which doesn't return COPY, the step is responsible
        # for writing to output_path itself. TODO we could check that something
        # has in fact been written to output_path in this case

        metadata = {"status": "valid"}
        self._write_metadata(metadata, i)

        # we'll only get here if Reject isn't raised.
        self.valid_items += 1


class InputStep(Step):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # for caching purposes
        self._items = None

    @abstractmethod
    def items(self):
        pass

    def item(self, item, i):
        if self._items is None:
            self._items = self.items()
        return self._items[i]


class FilterStep(Step):
    def item(self, item, i):
        if not self.filter(item):
            raise Reject()
        # filter steps don't modify the accepted items, so tell Funnel to copy
        # the item from the previous step. This differs from `return item` as
        # the item may be a path with a full directory structure (output = "path").
        return COPY

    @abstractmethod
    def filter(self, item):
        """
        Returns True if the item is valid and should be kept, and False
        otherwise.
        """


class Funnel:
    metadata_filename = "_statistics.json"

    def __init__(self, storage_dir):
        self.storage_dir = Path(storage_dir)
        # make sure the dir exists
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.steps = []
        # mapping of step to their parent
        self._parent_step = {}
        # for now, assume that steps are added in a linear hierarchy after their
        # parent. We can add tree parent hierarchies later.
        self._most_recently_added_step = None

    def _find_step(self, StepClass):
        for step in self.steps:
            if step.name == StepClass.name:
                return step
        raise Exception(f"could not find step {StepClass.name} (options: {self.steps})")

    def add_step(self, StepClass):
        parent_step = self._most_recently_added_step
        step = StepClass(self.storage_dir, parent_step=parent_step)
        # first step has to be an InputStep, and nothing else can be an InputStep.
        assert isinstance(step, InputStep) == (len(self.steps) == 0)

        self.steps.append(step)
        self._parent_step[step] = parent_step
        self._most_recently_added_step = step

    def run(self, *, argv=None, discovery_batch=False):
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
        """
        for step in self.steps:
            self._run_step(step, argv=argv, discovery_batch=discovery_batch)

    def run_from_step(self, /, StepClass):
        """
        Runs the funnel from (and including) the given step, but not any steps
        before.
        """
        step = self._find_step(StepClass)
        i = self.steps.index(step)
        for step in self.steps[i:]:
            self._run_step(step)

    def run_single_step(self, /, StepClass):
        step = self._find_step(StepClass)
        self._run_step(step)

    def _input(self, step, i):
        parent_step = self._parent_step[step]
        p = parent_step.output_path(i)
        # a step's input is the same as its parent step's output.
        input_type = parent_step.output
        if input_type == "object":
            with open(p) as f:
                val = f.read()
                return json.loads(val)
        if input_type == "path":
            return p

    def _collect_metadata(self, step: Step):
        metadata = {
            "count_items": 0,
            "count_rejected": 0,
            "rejected": [],
            "item_metadata": {},
        }
        for p in step.metadata_dir.glob("*"):
            # ignore .DS_Store and other garbage
            if not re.match(r"\d+", p.name):
                continue
            i = int(p.name)
            with open(p) as f:
                item_metadata = json.loads(f.read())

            metadata["count_items"] += 1
            if item_metadata["status"] == "rejected":
                metadata["count_rejected"] += 1
                metadata["rejected"].append(i)
            if item_metadata["metadata"]:
                metadata["item_metadata"][i] = item_metadata["metadata"]

        return metadata

    def _run_step(self, step: Step, *, argv=None, discovery_batch=False):
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
            items = step.items()
            # ...then (ab)use process_item to handle writing each item to its file
            # (if the InputStep has output == "object").
            for i, item in enumerate(items):
                step.process_item(item, i)
        else:
            parent_step = self._parent_step[step]
            # invariant: the names of the output files/dirs are ordered
            # sequentially (no breaks in the ordering).
            parent_step_count_items = highest_numbered_file(parent_step.storage_dir)
            # TODO modify this block for discovery_batch.
            # will need submit a batch job to discovery, then spinlock
            # checking its status every 30 seconds or so.

            if discovery_batch:
                if len(argv) == 1:
                    # nothing was passed, so this is the master process responsible
                    # for launching the array job.
                    f = argv[0]
                    user = getpass.getuser()
                    # https://rc-docs.northeastern.edu/en/latest/index.html
                    # %A = array job ID, %a = array index
                    # TODO fiogure out a better place for output and error logs
                    # in the array job. should we have a meta dir per Funnel
                    # where this stuff can go?
                    array_job = f"""
                        #!/bin/bash
                        #SBATCH --partition=short
                        #SBATCH --job-name {step.name}
                        #SBATCH --nodes 1
                        #SBATCH --ntasks 1
                        #SBATCH --array=0-{parent_step_count_items}%50   # array description (% is num simultaneous jobs, max 50)
                        #SBATCH -o /scratch/{user}/output_%A_%a.txt
                        #SBATCH -e /scratch/{user}/error_%A_%a.txt

                        python {f} $SLURM_ARRAY_TASK_ID
                    """
                    array_job = textwrap.dedent(array_job).strip()
                    with NamedTemporaryFile(mode="w+", suffix=".sh", delete=False) as f:
                        f.write(array_job)
                    subprocess.Popen(["sbatch", f.name])

                    while True:
                        time.sleep(30)
                        # piping to grep is annoying without shell=True. Should
                        # probably use proper subprocess.Pipe at some point for
                        # security though.
                        process = subprocess.run(f"squeue -u {user} | grep {step.name}")
                        # grep found something iff returncode is 0
                        if process.returncode != 0:
                            # grep didn't find anything, so the batch job finished.
                            break
                else:
                    # we're already inside a batch job and need to process a specific
                    # item on this step.
                    i = int(argv[1])
                    val = self._input(step, i)
                    step.process_item(val, i)
            else:
                # no discovery batch mode. execute normally (sequential execution).
                for i in range(parent_step_count_items + 1):
                    val = self._input(step, i)
                    step.process_item(val, i)

        ended_at = datetime.now(timezone.utc).timestamp()
        metadata = self._collect_metadata(step)
        metadata |= {"started_at": started_at, "ended_at": ended_at}
        with open(step.storage_dir / self.metadata_filename, "w+") as f:
            f.write(json.dumps(metadata))

        # now that we're done collecting metadata, we can remove that directory.
        # this avoids confusing when looking at the data; the intermediate step
        # is only necessary to accomodate the distributed nature of discovery.
        # this line can be commented out if needed for debugging.
        shutil.rmtree(step.metadata_dir)
