from pathlib import Path
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any
from datetime import datetime, timezone
import json


class Reject(Exception):
    """
    Raised when a step rejects (filters out) the current item.
    """


@dataclass
class Run:
    started_at: datetime
    ended_at: datetime
    items: list[Any]


class Step:
    # subclasses must set these at the class level
    name = None
    output = None
    input = None

    def __init__(self, storage_dir):
        # Written to statistics.json at the end of a step. steps can put anything
        # they want here, e.g. stacktraces for items that were rejected as a
        # result of errors.
        self.metadata = {}
        self.storage_dir = storage_dir / self.name
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

    def process_item(self, item, i):
        output = self.item(item, i)
        # we'll only get here if Reject isn't raised (that's handled by Funnel).
        self.valid_items += 1
        if self.output == "object":
            if output is None:
                raise Exception(
                    'Step must return a value from item() for output == "object"'
                )

            output = json.dumps(output)
            # use valid_items as it ensures sequentially ordering when filtering,
            # whereas i does not (i is the previous step's i)
            with open(self.output_path(self.valid_items), "w+") as f:
                f.write(output)
        # for output == "path", the step is responsible for writing to
        # output_path itself


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
        step = StepClass(self.storage_dir)
        # first step has to be an InputStep, and nothing else can be an InputStep.
        assert isinstance(step, InputStep) == (len(self.steps) == 0)

        self.steps.append(step)
        self._parent_step[step] = self._most_recently_added_step
        self._most_recently_added_step = step

    def run(self):
        for step in self.steps:
            self.run_step(step)

    def run_from_step(self, /, StepClass):
        """
        Runs the funnel from (and including) the given step, but not any steps
        before.
        """
        step = self._find_step(StepClass)
        i = self.steps.index(step)
        for step in self.steps[i:]:
            self.run_step(step)

    def run_single_step(self, /, StepClass):
        step = self._find_step(StepClass)
        self.run_step(step)

    def _input(self, step, i):
        parent_step = self._parent_step[step]
        p = parent_step.storage_dir / f"{i}"
        # a step's input is the same as its parent step's output.
        input_type = parent_step.output
        if input_type == "object":
            with open(p) as f:
                val = f.read()
                return json.loads(val)
        if input_type == "path":
            return p

    def run_step(self, step: Step) -> list[Any]:
        print(f"running step {step.name}")
        step.storage_dir.mkdir(exist_ok=True)

        items = []
        rejected = []
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
            i = 0
            # invariant: the names of the output files/dirs are ordered
            # sequentially (no breaks in the ordering).
            while (p := Path(parent_step.output_path(i))).exists():
                val = self._input(step, i)
                try:
                    step.process_item(val, i)
                except Reject:
                    rejected.append(i)
                    continue
                items.append(i)
                i += 1

        ended_at = datetime.now(timezone.utc).timestamp()
        metadata = {
            "started_at": started_at,
            "ended_at": ended_at,
            "count_items": len(items),
            "count_rejected": len(rejected),
            "rejected": rejected,
            "metadata": step.metadata,
        }
        with open(step.storage_dir / self.metadata_filename, "w+") as f:
            f.write(json.dumps(metadata))

        return items
