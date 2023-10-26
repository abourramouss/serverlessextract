from __future__ import annotations

import logging

from lithops import Storage
import time
import json
from copy import deepcopy

logger = logging.getLogger(__name__)


class _ContextManagerTimer:
    def __init__(self, key: str, stats: Stats):
        self.__key = key
        self.__stats = stats

    def __enter__(self):
        self.__stats.start_timer(self.__key)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__stats.stop_timer(self.__key)


class Stats:
    def __init__(self):
        self.__timers = {}
        self.__values = {}

    def start_timer(self, key):
        if key in self.__timers:
            logger.warning("Timer %s was already running, it will restart")
        self.__timers[key] = {"t0": time.time(), "t0_perf_counter": time.perf_counter()}

    def stop_timer(self, key):
        if key not in self.__timers:
            logger.warning("Timer %s not registered, skipping...")
            return

        self.__timers[key]["t1"] = time.time()
        self.__timers[key]["elapsed"] = (
            time.perf_counter() - self.__timers[key]["t0_perf_counter"]
        )
        del self.__timers[key]["t0_perf_counter"]

    def set_value(self, key, value):
        if key in self.__values:
            logger.warning("Value with key %s already exists, it will overwrite it")
        self.__values[key] = value

    def incr_value(self, key, delta=1):
        if key not in self.__values:
            self.__values[key] = delta
        else:
            self.__values[key] += delta

    def timeit(self, key):
        return _ContextManagerTimer(key, self)

    def dump_dict(self):
        return {"timers": self.__timers, "values": self.__values}
