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
    pass

# a step takes as input one of:
# - nothing
# - python object (repo name as string)
# - path to file / directory (cloned repository)
#
# and returns as output one of:
# - "invalid" (discard this item)
# - python object
# - path to file / directory

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

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    @abstractmethod
    def item(self, item, output_path):
        pass

    def output_path(self, i):
        return self.storage_dir / f"{i}"

    def process_item(self, item, i):
        self.item(item, i)
        # we'll only get here if Reject isn't raised (that's handled by Funnel).
        self.valid_items += 1
        if self.output == "object":
            # use valid_items as it ensures sequentially ordering when filtering,
            # whereas i does not (i is the previous step's i)
            with open(self.output_path(self.valid_items), "w+") as f:
                f.write(item)
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
    metadata_filename = "statistics.json"

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

    def add_step(self, StepClass):
        step = StepClass(self.storage_dir)
        self.steps.append(step)
        self._parent_step[step] = self._most_recently_added_step
        self._most_recently_added_step = step

    def run(self):
        # first step has to be an input step
        assert isinstance(self.steps[0], InputStep)
        previous_items = None
        for step in self.steps:
            previous_items = self.run_step(step, previous_items=previous_items)

    def _input(self, step, i):
        parent_step = self._parent_step[step]
        p = parent_step.storage_dir / f"{i}"
        if step.input == "object":
            with open(p) as f:
                return f.read()
        if step.input == "path":
            return p

    def run_step(self, step: Step, *, previous_items: list[Any] | None) -> list[Any]:
        print(f"running step {step.name}")
        step.storage_dir.mkdir(exist_ok=True)

        items = []
        rejected = []
        started_at = datetime.now(timezone.utc).timestamp()

        if previous_items is None:
            # this is the first (aka input) step
            assert isinstance(step, InputStep)
            # this is ugly and should be refactored (though I'm not sure
            # how yet). We need the InputStep to write its outputs in the correct
            # way, hence the process_item loop.
            items = step.items()
            for i, item in enumerate(items):
                step.process_item(item, i)
        else:
            for i in range(len(previous_items)):
                val = self._input(step, i)
                try:
                    step.process_item(val, i)
                except Reject:
                    rejected.append(i)
                    continue
                items.append(i)

        ended_at = datetime.now(timezone.utc).timestamp()
        metadata = {
            "started_at": started_at,
            "ended_at": ended_at,
            "count_items": len(items),
            "count_rejected": len(rejected),
            "rejected": rejected,
            "metadata": step.metadata
        }
        with open(step.storage_dir / self.metadata_filename, "w+") as f:
            f.write(json.dumps(metadata))

        return items
