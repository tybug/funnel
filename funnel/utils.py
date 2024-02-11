from pathlib import Path
import re


def highest_numbered_file(path: Path):
    n = 0
    for p in path.glob("*"):
        # we explicitly allow both files and directories here, because steps
        # may output either
        if not re.match(r"\d+", p.name):
            continue
        n = max(n, int(p.name))

    return n
