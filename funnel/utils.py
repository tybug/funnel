from json import JSONEncoder as _JSONEncoder
import json
import dataclasses
from collections import UserList


# support dumping dataclasses to json.
class JsonEncoder(_JSONEncoder):
    def default(self, obj):
        if dataclasses.is_dataclass(obj):
            return dataclasses.asdict(obj)
        return super().default(obj)


def dumps(obj, *args, **kwargs):
    kwargs["cls"] = JsonEncoder
    return json.dumps(obj, *args, **kwargs)


class TrackedClasses(UserList):
    def __init__(self, *, ignore_classes=[]):
        self.ignore_classes = ignore_classes
        super().__init__()


class TrackSubclassesMeta(type):
    def __init__(cls, name, bases, attrs):
        # filter out current class
        mro = [c for c in cls.__mro__ if c != cls]
        # traverse up inheritance hierarchy, looking for a base class
        # (which here means a class with a TrackedClasses attribute)
        for class_ in mro:
            for name_, value in class_.__dict__.items():
                if isinstance(value, TrackedClasses):
                    tracked_classes = getattr(class_, name_)
                    # check tracked classes blacklist first
                    if name in tracked_classes.ignore_classes:
                        continue

                    tracked_classes.append(cls)

        super().__init__(name, bases, attrs)
