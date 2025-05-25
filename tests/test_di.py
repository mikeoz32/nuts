from typing import Annotated
from nuts.di import Bean


def get_integer():
    return 1


def no_params(c: Annotated[int, Bean(get_integer)]) -> int:
    return 1 + c


def fn(
    a: int, b: Annotated[int, Bean(no_params)], c: Annotated[int, Bean(get_integer)]
):
    return a + b + c


def test_bean():
    b = Bean(fn)
    assert b(a=2) == 5
