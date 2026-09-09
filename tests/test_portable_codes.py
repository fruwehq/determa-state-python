from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

import pytest

import determa.state as ds


def test_portable_code_sets_are_deeply_immutable() -> None:
    assert isinstance(ds.PORTABLE_CODE_SETS, Mapping)
    assert all(isinstance(codes, frozenset) for codes in ds.PORTABLE_CODE_SETS.values())

    mutable_view = cast(Any, ds.PORTABLE_CODE_SETS)
    with pytest.raises(TypeError):
        mutable_view["disposition"] = frozenset()
    with pytest.raises(AttributeError):
        mutable_view["disposition"].add("unknown")


def test_nonportable_categories_are_not_exported() -> None:
    assert "execution_store_failure" not in ds.PORTABLE_CODE_SETS
    assert "structural_validation" not in ds.PORTABLE_CODE_SETS
