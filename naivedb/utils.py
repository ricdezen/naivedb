import time
from typing import Callable, Optional, Any


def timer(log: Callable[[str], Optional[Any]] = print):
    """
    Print execution time to `log` function.

    :param log: The function to log the result to.
    """

    def timer_decorator(func):
        def timed(*args, **kwargs):
            # Run the function and time it.
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()

            # Print to log function.
            log(f"Function {func} took {end_time - start_time} seconds to run.")

            return result

        return timed

    return timer_decorator
