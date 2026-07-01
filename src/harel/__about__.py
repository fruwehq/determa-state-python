"""Single source of truth for the package version.

Both the runtime (``harel.__version__``) and the build backend (hatchling, via
``[tool.hatch.version]`` in ``pyproject.toml``) read the version from here, so there
is exactly one place to bump.
"""

__version__ = "0.0.3"
