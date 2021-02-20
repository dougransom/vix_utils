
import time as time
import logging
import functools


# adapted from from https://medium.com/pythonhive/python-decorator-to-measure-the-execution-time-of-methods-fa04cb6bb36d
def timeit(log_level=logging.DEBUG, limit_arg_chars=100):
    """

    :param log_level:
    :param limit_arg_chars: the maximum number of characters of the arguments to log.
    :return: a function that can be used as a decorator, to time the length of a function call and
    log the results with the appropriate log_level.
    """

    def decorator_timeit(method):
        @functools.wraps(method)
        def timed(*args, **kw):
            ts = time.perf_counter()
            result = method(*args, **kw)
            te = time.perf_counter()
            elapsed = te-ts
            # skip kw for now
            # only the first limit_arg_chars are displayed characters of args
            args_str = f"{args}"[0:limit_arg_chars]

            message = f"Elapsed time {elapsed} for {method.__name__}  {args_str}"
            logging.log(log_level, message)
            return result
        return timed

    return decorator_timeit


if __name__ == "__main__":
    @timeit(logging.WARN)
    def a():
        time.sleep(1)
    print("Timeit")
    a()
