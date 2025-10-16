from functools import wraps
from logging import getLogger
from typing import Callable, ParamSpec, TypeVar

from sqlalchemy.exc import (
    DBAPIError,
    IntegrityError,
    ProgrammingError,
    TimeoutError,
)

T = TypeVar("T")
P = ParamSpec("P")


def handle_db_exceptions(func: Callable[P, T]) -> Callable[P, T]:
    @wraps(func)
    async def wrapper(*args, **kwargs):
        logger = getLogger(func.__module__)
        try:
            return await func(*args, **kwargs)
        except IntegrityError as ex:
            logger.error(f"Integrity error in function {func.__name__}: {ex}.")
            raise ValueError(f"{ex.orig}, {ex.params}, {ex.statement}")
        except ProgrammingError as ex:
            logger.error(
                f"Programming error in function {func.__name__}: {ex}."
            )
            raise ValueError(f"{ex.orig}, {ex.params}, {ex.statement}")
        except DBAPIError as ex:
            msg = "Incorrect user input. "
            logger.error(f"{msg}. Original error {ex.orig}.")
            raise ValueError("Incorrect user input.")
        except TimeoutError:
            msg = "Can't connect to DB. Timeout Error"
            logger.error(f"{msg}.")
            raise ValueError("Can't connect to DB. Timeout Error")
        except Exception as ex:
            logger.error(f"Unexpected error in function {func.__name__}: {ex}.")
            raise Exception(f"{type(ex)}: {ex}.")

    return wrapper
