"""Executable anchors captured during trusted package initialization.

The interpreter, standard-library installation and this initialization must be
trusted. These anchors detect later rebinding; they are not a Python sandbox or
proof of a native library's implementation. Hosts must exclude concurrent code
mutation while verifying and using an extension.
"""

from __future__ import annotations

import importlib
import sys
import types
from typing import Any

# Load the supported store/verifier platform before capturing its bindings. This
# deliberately excludes optional third-party drivers such as psycopg.
for _name in (
    "abc",
    "ast",
    "collections.abc",
    "contextlib",
    "copy",
    "dataclasses",
    "enum",
    "functools",
    "hashlib",
    "json",
    "pathlib",
    "re",
    "sqlite3",
    "sysconfig",
    "threading",
    "typing",
    "urllib.parse",
    "weakref",
):
    importlib.import_module(_name)


class _PlatformBindings:
    def __init__(self) -> None:
        self.modules = {
            name: (module, dict(vars(module)))
            for name, module in tuple(sys.modules.items())
            if type(module) is types.ModuleType and name.split(".", 1)[0] in sys.stdlib_module_names
        }
        self.functions: dict[Any, tuple[Any, ...]] = {}
        self.classes: dict[type, tuple[Any, ...]] = {}
        self.descriptors: dict[int, tuple[Any, ...]] = {}
        for _, namespace in self.modules.values():
            for value in namespace.values():
                self._capture(value)

    def _capture(self, value: Any) -> None:
        if type(value) is types.FunctionType:
            if value in self.functions:
                return
            self.functions[value] = (
                value.__code__,
                value.__globals__,
                value.__defaults__,
                dict(value.__kwdefaults__ or {}),
                tuple(cell.cell_contents for cell in value.__closure__ or ()),
                dict(value.__globals__),
            )
            for cell in value.__closure__ or ():
                self._capture(cell.cell_contents)
        elif isinstance(value, type):
            if value in self.classes:
                return
            namespace = dict(vars(value))
            self.classes[value] = (type(value), value.__bases__, namespace)
            self._capture(type(value))
            for base in value.__bases__:
                self._capture(base)
            for member in namespace.values():
                self._capture(member)
        elif type(value) in (property, staticmethod, classmethod):
            members = (
                (value.fget, value.fset, value.fdel)
                if type(value) is property
                else (value.__func__,)
            )
            self.descriptors[id(value)] = members
            for member in members:
                self._capture(member)

    def matches(self, module: types.ModuleType, attribute: str) -> bool:
        seen: set[int] = set()

        def executable(value: Any) -> bool:
            return callable(value) or isinstance(value, (property, staticmethod, classmethod))

        def walk(value: Any) -> bool:
            if id(value) in seen:
                return True
            seen.add(id(value))
            if type(value) is types.FunctionType:
                saved = self.functions.get(value)
                if saved is None:
                    return False
                code, namespace, defaults, kwdefaults, closure, saved_namespace = saved
                if (
                    value.__code__ is not code
                    or value.__globals__ is not namespace
                    or value.__defaults__ is not defaults
                    or (value.__kwdefaults__ or {}).keys() != kwdefaults.keys()
                    or any(
                        (value.__kwdefaults__ or {})[key] is not item
                        for key, item in kwdefaults.items()
                    )
                ):
                    return False
                current_closure = tuple(cell.cell_contents for cell in value.__closure__ or ())
                if len(current_closure) != len(closure):
                    return False
                for current, original in zip(current_closure, closure, strict=True):
                    if executable(current) or executable(original):
                        if current is not original or not walk(current):
                            return False
                # Include nested code: comprehensions and returned wrappers share
                # this global namespace even before a wrapper has been created.
                pending = [code]
                names: set[str] = set()
                while pending:
                    current_code = pending.pop()
                    names.update(current_code.co_names)
                    pending.extend(
                        item for item in current_code.co_consts if type(item) is types.CodeType
                    )
                for name in names & saved_namespace.keys():
                    original = saved_namespace[name]
                    current = namespace.get(name)
                    if (
                        executable(original)
                        or executable(current)
                        or type(original) is types.ModuleType
                    ):
                        if current is not original or not walk(current):
                            return False
                        if type(original) is types.ModuleType:
                            module_snapshot = next(
                                (item[1] for item in self.modules.values() if item[0] is original),
                                None,
                            )
                            if module_snapshot is None:
                                return False
                            for attribute in names & module_snapshot.keys():
                                member = module_snapshot[attribute]
                                selected = vars(original).get(attribute)
                                if executable(member) or executable(selected):
                                    if selected is not member or not walk(selected):
                                        return False
                return True
            if isinstance(value, type):
                saved_class = self.classes.get(value)
                if saved_class is None:
                    return False
                metaclass, bases, namespace = saved_class
                if type(value) is not metaclass or value.__bases__ != bases:
                    return False
                current_namespace = vars(value)
                for name in current_namespace.keys() | namespace.keys():
                    original = namespace.get(name)
                    current = current_namespace.get(name)
                    if executable(original) or executable(current):
                        if current is not original or not walk(current):
                            return False
                return all(walk(base) for base in bases) and walk(metaclass)
            if type(value) in (property, staticmethod, classmethod):
                members = self.descriptors.get(id(value))
                return members is not None and all(walk(member) for member in members)
            if type(value) is types.ModuleType:
                saved_module = next(
                    (item for item in self.modules.values() if item[0] is value), None
                )
                if saved_module is None or saved_module[0] is not value:
                    return False
                if not any(
                    sys.modules.get(name) is value
                    for name, item in self.modules.items()
                    if item[0] is value
                ):
                    return False
                return True
            return True

        saved_module = self.modules.get(module.__name__)
        return (
            saved_module is not None
            and saved_module[0] is module
            and sys.modules.get(module.__name__) is module
            and attribute in saved_module[1]
            and vars(module).get(attribute) is saved_module[1][attribute]
            and walk(saved_module[1][attribute])
        )


PLATFORM_BINDINGS = _PlatformBindings()
