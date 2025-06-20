def create_class(class_path: str):
    mp = module_path(class_path)
    cn = class_name(class_path)
    mod = __import__(mp, fromlist=[cn])
    class_ = getattr(mod, cn)
    return class_


def module_path(class_path: str):
    return ".".join(class_path.split(".")[0:-1])


def class_name(class_path: str):
    return class_path.split(".")[-1]

def fqn(_class: type):
    return f"{_class.__class__.__module__}.{_class.__class__.__name__}"
