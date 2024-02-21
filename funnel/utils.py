from pathlib import Path
import re
from json import JSONEncoder as _JSONEncoder
import json
import dataclasses


# all the numbered files in a directory, representing the output of individual
# items from a step.
def item_ids_in_dir(path: Path):
    ids = []
    for p in path.glob("*"):
        # we explicitly allow both files and directories here, because steps
        # may output either
        if not re.match(r"\d+", p.name):
            continue
        ids.append(int(p.name))

    return ids


# support dumping dataclasses to json.
class JsonEncoder(_JSONEncoder):
    def default(self, obj):
        if dataclasses.is_dataclass(obj):
            return dataclasses.asdict(obj)
        return super().default(obj)


def dumps(obj, *args, **kwargs):
    kwargs["cls"] = JsonEncoder
    return json.dumps(obj, *args, **kwargs)
