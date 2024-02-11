from pathlib import Path
import re


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
