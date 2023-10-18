from functools import wraps
import time

# Wrapper that wraps around a function and returns the time it took to execute the function (Mainly used for I/O operations)


# Wrapper that wraps around a function and returns the time it took to execute the function (Mainly used for I/O operations)
def timeit_io(method):
    @wraps(method)
    def timed(*args, **kw):
        start_time = time.time()
        method(*args, **kw)
        end_time = time.time()
        elapsed_time = end_time - start_time
        return elapsed_time

    return timed


# Wrapper that wraps around a function and returns the time it took to execute the function (Mainly used for execution time)
def timeit_execution(method):
    @wraps(method)
    def timed(*args, **kw):
        start_time = time.time()
        method(*args, **kw)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"{method.__name__}  {elapsed_time} seconds")
        return elapsed_time

    return timed
