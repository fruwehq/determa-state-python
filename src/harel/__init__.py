"""harel — Python reference implementation of the harel statechart engine.

The normative SPEC.md, machine JSON Schema, and cross-language conformance
suite live in the spec repo (https://github.com/fruwehq/harel). This package
implements that spec; it is correct iff it passes the conformance suite.
"""

from __future__ import annotations

from . import yaml12
from .cel import CelError
from .definition import Definition, load_definition, load_definitions
from .engine import Host
from .errors import ErrorRecord, HarelError, SchemaError, ValidationError
from .instance import Event, Instance, Status
from .model import Machine, State
from .validator import collect_errors, validate

__all__ = [
    "Definition",
    "ErrorRecord",
    "Event",
    "HarelError",
    "Host",
    "Instance",
    "Machine",
    "SchemaError",
    "State",
    "Status",
    "CelError",
    "ValidationError",
    "collect_errors",
    "load_definition",
    "load_definitions",
    "validate",
    "yaml12",
    "__version__",
]

__version__ = "0.0.1"
