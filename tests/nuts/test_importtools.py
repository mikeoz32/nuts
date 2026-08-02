from nuts.server import importtools


class Example:
    pass


def test_create_class_and_fqn():
    cls = importtools.create_class("tests.nuts.test_importtools.Example")
    assert cls.__name__ == "Example"

    instance = Example()
    assert importtools.fqn(instance).endswith(".Example")
