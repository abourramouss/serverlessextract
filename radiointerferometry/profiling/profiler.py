import psutil
import time
import os
from multiprocessing import Process, Pipe
import contextlib
import json
import requests
from dataclasses import dataclass, asdict, fields


def time_it(label, function, time_records, *args, **kwargs):
    print(f"label: {label}, type of function: {type(function)}")

    start_time = time.time()
    result = function(*args, **kwargs)
    end_time = time.time()

    record = FunctionTimer(label, start_time, end_time, (end_time - start_time))
    time_records.append(record)

    return result


@contextlib.contextmanager
def profiling_context(monitored_process_pid):
    parent_conn, child_conn = Pipe()
    profiler = Profiler()
    monitoring_process = Process(
        target=profiler.start_profiling, args=(child_conn, monitored_process_pid)
    )
    monitoring_process.start()
    try:
        yield profiler
    finally:
        parent_conn.send("stop")
        monitoring_process.join(timeout=10)
        if parent_conn.poll(timeout=1):  # Add a timeout to the recv() call
            received_profiler_data = parent_conn.recv()
            profiler.update(received_profiler_data)
        else:
            print(
                "No data received from the profiling process within the timeout period."
            )
        if monitoring_process.is_alive():
            monitoring_process.terminate()
        parent_conn.close()
        child_conn.close()


@dataclass
class FunctionTimer:
    label: str
    start_time: float
    end_time: float
    duration: float

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    def __repr__(self) -> str:
        return f"FunctionTimer(label={self.label}, start_time={self.start_time}, end_time={self.end_time}, duration={self.duration})"


@dataclass
class BaseMetric:
    timestamp: float

    def __lt__(self, other):
        if not isinstance(other, BaseMetric):
            return NotImplemented
        return self.timestamp < other.timestamp

    def __add__(self, other):
        if not isinstance(other, type(self)):
            raise TypeError(
                f"Cannot add different metric types: {type(self).__name__} and {type(other).__name__}"
            )

        new_data = {"timestamp": 0}

        if hasattr(self, "collection_id"):
            new_data["collection_id"] = self.collection_id

        if hasattr(self, "pid"):
            new_data["pid"] = 0

        # Handle summing or retaining of specific metric fields
        for field in fields(self):
            if field.name not in new_data:
                if field.name in [
                    "cpu_usage",
                    "memory_usage",
                    "disk_read_mb",
                    "disk_write_mb",
                    "disk_read_rate",
                    "disk_write_rate",
                    "net_read_mb",
                    "net_write_mb",
                ]:
                    field_value = getattr(self, field.name) + getattr(other, field.name)
                    new_data[field.name] = field_value

        return self.__class__(**new_data)

    def __sub__(self, other):
        if not isinstance(other, type(self)):
            raise TypeError(
                f"Cannot subtract different metric types: {type(self).__name__} and {type(other).__name__}"
            )

        new_data = {"timestamp": 0}

        if hasattr(self, "collection_id"):
            new_data["collection_id"] = self.collection_id

        if hasattr(self, "pid"):
            new_data["pid"] = 0

        for field in fields(self):
            if field.name not in new_data:
                if field.name in [
                    "cpu_usage",
                    "memory_usage",
                    "disk_read_mb",
                    "disk_write_mb",
                    "net_read_mb",
                    "net_write_mb",
                ]:

                    field_value = getattr(self, field.name) - getattr(other, field.name)
                    new_data[field.name] = field_value

        return self.__class__(**new_data)

    def __truediv__(self, number):
        if not isinstance(number, (int, float)):
            return NotImplemented
        for field in fields(self):
            if field.name in [
                "cpu_usage",
                "memory_usage",
                "disk_read_mb",
                "disk_write_mb",
                "net_read_mb",
                "net_write_mb",
            ]:

                field_value = getattr(self, field.name) / number
                setattr(self, field.name, field_value)

        return self

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    def __repr__(self):
        return f"{self.__class__.__name__}({', '.join(f'{field.name}={getattr(self, field.name)}' for field in fields(self))})"


@dataclass
class CPUMetric(BaseMetric):
    collection_id: int
    pid: int
    cpu_usage: float


@dataclass
class MemoryMetric(BaseMetric):
    collection_id: int
    pid: int
    memory_usage: float


@dataclass
class DiskMetric(BaseMetric):
    collection_id: int
    pid: int
    disk_read_mb: float
    disk_write_mb: float
    disk_read_rate: float
    disk_write_rate: float


@dataclass
class NetworkMetric(BaseMetric):
    collection_id: int
    net_read_mb: float
    net_write_mb: float
    net_read_rate: float
    net_write_rate: float


class IMetricCollector:
    def collect_metric(self, pid, collection_id):
        current_time = time.time()
        return self._collect(pid, current_time, collection_id)

    def _collect(self, pid, timestamp):
        raise NotImplementedError


class CPUMetricCollector(IMetricCollector):
    def _collect(self, pid, timestamp, collection_id):
        try:

            cpu_usage = psutil.Process(pid).cpu_percent(interval=0.01)
            return CPUMetric(
                timestamp=timestamp,
                pid=pid,
                cpu_usage=cpu_usage,
                collection_id=collection_id,
            )
        except psutil.NoSuchProcess:
            print(f"Process with PID {pid} no longer exists.")


class MemoryMetricCollector(IMetricCollector):
    def _collect(self, pid, timestamp, collection_id):
        try:
            memory_usage = psutil.Process(pid).memory_info().rss >> 20  # Convert to MB
            return MemoryMetric(
                timestamp=timestamp,
                pid=pid,
                memory_usage=memory_usage,
                collection_id=collection_id,
            )
        except psutil.NoSuchProcess:
            print(f"Process with PID {pid} no longer exists.")


class DiskMetricCollector(IMetricCollector):
    def _collect(self, pid, timestamp, collection_id):
        try:
            current_counter = psutil.Process(pid).io_counters()
            disk_read_mb = current_counter.read_bytes / 1024.0**2  # Convert to MB
            disk_write_mb = current_counter.write_bytes / 1024.0**2  # Convert to MB
            return DiskMetric(
                timestamp=timestamp,
                pid=pid,
                disk_read_mb=disk_read_mb,
                disk_write_mb=disk_write_mb,
                collection_id=collection_id,
            )
        except psutil.NoSuchProcess:
            print(f"Process with PID {pid} no longer exists.")


class NetworkMetricCollector(IMetricCollector):
    def _collect(self, pid, timestamp, collection_id):
        current_net_counters = psutil.net_io_counters(pernic=False)
        net_read_mb = current_net_counters.bytes_recv / 1024.0**2  # Convert to MB
        net_write_mb = current_net_counters.bytes_sent / 1024.0**2  # Convert to MB
        return NetworkMetric(
            timestamp=timestamp,
            net_read_mb=net_read_mb,
            net_write_mb=net_write_mb,
            collection_id=collection_id,
        )


@dataclass
class MetricCollector:
    def __init__(self):
        self.cpu_collector = CPUMetricCollector()
        self.memory_collector = MemoryMetricCollector()
        self.disk_collector = DiskMetricCollector()
        self.network_collector = NetworkMetricCollector()
        self.cpu_metrics = []
        self.memory_metrics = []
        self.disk_metrics = []
        self.network_metrics = []

    def __len__(self):
        min_len = min(
            len(self.cpu_metrics), len(self.memory_metrics), len(self.disk_metrics)
        )
        return min_len

    def __iter__(self):
        for metric in self.cpu_metrics:
            yield ("cpu", metric)
        for metric in self.memory_metrics:
            yield ("memory", metric)
        for metric in self.disk_metrics:
            yield ("disk", metric)
        for metric in self.network_metrics:
            yield ("network", metric)

    @classmethod
    def from_dict(cls, data):
        instance = cls()
        instance.cpu_metrics = [
            CPUMetric(**metric) for metric in data.get("cpu_metrics", [])
        ]
        instance.memory_metrics = [
            MemoryMetric(**metric) for metric in data.get("memory_metrics", [])
        ]
        instance.disk_metrics = [
            DiskMetric(**metric) for metric in data.get("disk_metrics", [])
        ]
        instance.network_metrics = [
            NetworkMetric(**metric) for metric in data.get("network_metrics", [])
        ]
        return instance

    def __repr__(self):
        return f"MetricCollector(cpu_metrics={self.cpu_metrics}, memory_metrics={self.memory_metrics}, disk_metrics={self.disk_metrics}, network_metrics={self.network_metrics})"

    def collect_all_metrics(self, parent_pid, index):
        current_process = psutil.Process(parent_pid)
        children = current_process.children(recursive=True)

        process_list = [current_process] + children

        # print(f"Tracking process PIDs: {[proc.pid for proc in process_list]}")

        for proc in process_list:
            pid = proc.pid

            # Collect Memory Metrics
            memory_metric = self.memory_collector.collect_metric(pid, index)
            if memory_metric is not None:
                self.memory_metrics.append(memory_metric)

            # Collect Disk Metrics
            timestamp = time.time()
            current_counter = psutil.Process(pid).io_counters()
            disk_read_mb = current_counter.read_bytes / 1024.0**2
            disk_write_mb = current_counter.write_bytes / 1024.0**2

            # Initialize rates as zero
            disk_read_rate_mb = 0
            disk_write_rate_mb = 0

            # Find the previous metric for the same PID, if it exists
            prev_disk_metric = next(
                (metric for metric in reversed(self.disk_metrics) if metric.pid == pid),
                None,
            )

            if prev_disk_metric:
                # Calculate time difference
                time_diff = timestamp - prev_disk_metric.timestamp

                # Ensure positive time difference
                if time_diff > 0:
                    # Calculate disk read and write rates
                    disk_read_rate_mb = (
                        disk_read_mb - prev_disk_metric.disk_read_mb
                    ) / time_diff
                    disk_write_rate_mb = (
                        disk_write_mb - prev_disk_metric.disk_write_mb
                    ) / time_diff

            # Create and append the new disk metric
            disk_metric = DiskMetric(
                timestamp=timestamp,
                pid=pid,
                disk_read_mb=disk_read_mb,
                disk_write_mb=disk_write_mb,
                disk_read_rate=disk_read_rate_mb,
                disk_write_rate=disk_write_rate_mb,
                collection_id=index,
            )
            self.disk_metrics.append(disk_metric)

            # Collect CPU Metrics
            cpu_metric = self.cpu_collector.collect_metric(pid, index)
            if cpu_metric is not None:
                self.cpu_metrics.append(cpu_metric)

        # Collect Network Metrics, these are global
        current_net_counters = psutil.net_io_counters(pernic=False)
        net_read_mb = current_net_counters.bytes_recv / 1024.0**2
        net_write_mb = current_net_counters.bytes_sent / 1024.0**2
        if self.network_metrics:
            prev_network_metric = self.network_metrics[-1]
            time_diff = timestamp - prev_network_metric.timestamp
            net_read_rate_mb = (
                (net_read_mb - prev_network_metric.net_read_mb) / time_diff
                if time_diff
                else 0
            )
            net_write_rate_mb = (
                (net_write_mb - prev_network_metric.net_write_mb) / time_diff
                if time_diff
                else 0
            )
        else:
            net_read_rate_mb = net_write_rate_mb = 0

        network_metric = NetworkMetric(
            timestamp=timestamp,
            net_read_mb=net_read_mb,
            net_write_mb=net_write_mb,
            net_read_rate=net_read_rate_mb,
            net_write_rate=net_write_rate_mb,
            collection_id=index,
        )
        self.network_metrics.append(network_metric)

        time.sleep(1)

    def update(self, received_data):
        if not isinstance(received_data, MetricCollector):
            raise ValueError("Received data is not an instance of MetricCollector")

        self.cpu_metrics.extend(received_data.cpu_metrics)
        self.memory_metrics.extend(received_data.memory_metrics)
        self.disk_metrics.extend(received_data.disk_metrics)
        self.network_metrics.extend(received_data.network_metrics)

    def to_dict(self):
        return {
            "cpu_metrics": [metric.to_dict() for metric in self.cpu_metrics],
            "memory_metrics": [metric.to_dict() for metric in self.memory_metrics],
            "disk_metrics": [metric.to_dict() for metric in self.disk_metrics],
            "network_metrics": [metric.to_dict() for metric in self.network_metrics],
        }


class Profiler:
    def __init__(self):
        self.worker_id = None
        self.worker_start_tstamp = None
        self.worker_end_tstamp = None
        self.metrics = MetricCollector()
        self.function_timers = []

    def __len__(self):
        return len(self.metrics)

    def __iter__(self):
        for metric in self.metrics:
            yield metric

    @classmethod
    def from_dict(cls, data):
        profiler = cls()

        if "metrics" in data:
            # If 'metrics' key is present, this is the expected structure
            profiler.metrics = MetricCollector.from_dict(data["metrics"])
        else:
            # If 'metrics' key is not present, handle data as metrics directly
            profiler.metrics = MetricCollector.from_dict(data)

        if "function_timers" in data:
            profiler.function_timers = [
                FunctionTimer.from_dict(timer) for timer in data["function_timers"]
            ]

        if "worker_id" in data:
            profiler.worker_id = data["worker_id"]

        if "worker_start_tstamp" in data:
            profiler.worker_start_tstamp = data["worker_start_tstamp"]

        if "worker_end_tstamp" in data:
            profiler.worker_end_tstamp = data["worker_end_tstamp"]

        return profiler

    def __repr__(self):
        return f"Profiler(worker_id={self.worker_id}, worker_start_tstamp={self.worker_start_tstamp}, worker_end_tstamp={self.worker_end_tstamp}, metrics={self.metrics}, function_timers={self.function_timers})"

    def start_profiling(self, conn, monitored_process_pid):
        index = 0
        try:
            while True:
                self.metrics.collect_all_metrics(monitored_process_pid, index)
                index += 1
                if conn.poll():
                    message = conn.recv()
                    if message == "stop":
                        print(
                            "Received stop signal, completing current data collection."
                        )
                        conn.send(self)
                        break
        except Exception as e:
            print(f"Exception in profiling process: {e}")
        finally:
            conn.close()

    def update(self, received_data):
        if not isinstance(received_data, Profiler):
            raise ValueError("Received data is not an instance of Profiler")

        self.metrics.update(received_data.metrics)

    def to_dict(self):
        return {
            "worker_id": self.worker_id,
            "worker_start_tstamp": self.worker_start_tstamp,
            "worker_end_tstamp": self.worker_end_tstamp,
            "metrics": self.metrics.to_dict(),
            "function_timers": [timer.to_dict() for timer in self.function_timers],
        }

    def to_json(self):
        return json.dumps(self.to_dict())
