"""Logging helpers for DLSlime.

Library modules should get loggers through this module instead of configuring
Python logging directly. Applications can opt in with:

    import logging
    import dlslime

    dlslime.set_log_level(logging.INFO)
"""

from __future__ import annotations

import logging as _logging

_ROOT_LOGGER_NAME = "slime"
_DEFAULT_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def get_logger(name: str | None = None) -> _logging.Logger:
    """Return a DLSlime logger.

    Passing ``"rpc"`` returns ``"slime.rpc"``. Passing an already qualified
    ``"slime.*"`` name returns it unchanged.
    """
    if not name:
        logger_name = _ROOT_LOGGER_NAME
    elif name == _ROOT_LOGGER_NAME or name.startswith(f"{_ROOT_LOGGER_NAME}."):
        logger_name = name
    else:
        logger_name = f"{_ROOT_LOGGER_NAME}.{name}"
    return _logging.getLogger(logger_name)


def set_log_level(
    level: int | str,
    *,
    logger_name: str = _ROOT_LOGGER_NAME,
    configure_handler: bool = True,
) -> _logging.Logger:
    """Set the log level for DLSlime loggers.

    ``configure_handler`` installs a stderr ``StreamHandler`` only when the
    target logger has no handlers yet. This keeps DLSlime quiet by default while
    making ``dlslime.set_log_level(logging.INFO)`` useful in scripts.
    """
    logger = get_logger(logger_name)
    logger.setLevel(level)
    has_real_handler = any(
        not isinstance(handler, _logging.NullHandler) for handler in logger.handlers
    )
    if configure_handler and not has_real_handler:
        handler = _logging.StreamHandler()
        handler.setFormatter(_logging.Formatter(_DEFAULT_FORMAT))
        logger.addHandler(handler)
    return logger


get_logger().addHandler(_logging.NullHandler())
