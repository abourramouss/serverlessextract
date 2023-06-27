from functools import wraps
import time


class Timer:
    def __init__(self):
        self.start = None
        self.end = None
        self.data = []

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, type, value, traceback):
        self.end = time.time()
        elapsed_time = self.end - self.start if self.end and self.start else None
        self.data.append(elapsed_time)

    @property
    def elapsed(self):
        return self.end - self.start if self.end and self.start else None


def timeit_io(method):
    @wraps(method)
    def timed(*args, **kw):
        timer = Timer()
        with timer:
            result = method(*args, **kw)
        return result, timer.data
    return timed


def timeit_execution(method):
    @wraps(method)
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print(f"{method.__name__}  {(te - ts) * 1000} ms")
        return (te - ts) * 1000  # Return only elapsed time in milliseconds
    return timed
