import pytest


def test_fake():
    with pytest.raises(ZeroDivisionError):
        result = 1 / 0
