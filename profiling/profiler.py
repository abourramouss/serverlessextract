import psutil
import time
import os
from multiprocessing import Process, Pipe
import contextlib
import json
from dataclasses import dataclass, asdict, fields


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
        new_data = {
            field.name: (getattr(self, field.name) + getattr(other, field.name)) / 2
            for field in fields(self)
        }
        return self.__class__(**new_data)

    def __truediv__(self, number):
        if not isinstance(number, (int, float)):
            return NotImplemented
        new_data = {
            field.name: getattr(self, field.name) / number for field in fields(self)
        }
        return self.__class__(**new_data)

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    def __repr__(self):
        return f"{self.__class__.__name__}({', '.join(f'{field.name}={getattr(self, field.name)}' for field in fields(self))})"


def time_it(label, function, time_records, *args, **kwargs):
    print(f"label: {label}, type of function: {type(function)}")

    start_time = time.time()
    result = function(*args, **kwargs)
    end_time = time.time()

    record = FunctionTimer(label, start_time, end_time, (end_time - start_time))
    time_records.append(record)

    return result


@contextlib.contextmanager
def profiling_context():
    parent_conn, child_conn = Pipe()
    profiler = Profiler()
    parent_pid = os.getpid()
    monitoring_process = Process(
        target=profiler.start_profiling, args=(child_conn, parent_pid)
    )
    monitoring_process.start()

    try:
        yield profiler
    finally:
        parent_conn.send("stop")
        received_profiler_data = parent_conn.recv()
        profiler.update(received_profiler_data)
        monitoring_process.join()
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


@dataclass
class NetworkMetric(BaseMetric):
    collection_id: int
    net_read_mb: float
    net_write_mb: float


class IMetricCollector:
    def collect_metric(self, pid, collection_id):
        current_time = time.time()
        return self._collect(pid, current_time, collection_id)

    def _collect(self, pid, timestamp):
        raise NotImplementedError


class CPUMetricCollector(IMetricCollector):
    def _collect(self, pid, timestamp, collection_id):
        try:
            cpu_usage = psutil.Process(pid).cpu_percent(interval=0.3)
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


class ProcessManager:
    def __init__(self, parent_pid):
        self.parent_pid = parent_pid

    def __repr__(self):
        return f"ProcessManager(parent_pid={self.parent_pid}) children: {self.get_processes_pids()}"

    def get_processes_pids(self):
        # Return all the children except the profiler process
        processes = []
        profiler_pid = os.getpid()

        processes.append(self.parent_pid)
        for p in psutil.Process(self.parent_pid).children(recursive=True):
            if p.pid == profiler_pid:
                continue
            processes.append(p.pid)
        return processes


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
            yield metric
        for metric in self.memory_metrics:
            yield metric
        for metric in self.disk_metrics:
            yield metric
        for metric in self.network_metrics:
            yield metric

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
        process_manager = ProcessManager(parent_pid)
        print(f"Tracking process PIDs: {process_manager.get_processes_pids()}")
        network_metric = self.network_collector.collect_metric(parent_pid, index)
        if network_metric is not None:
            self.network_metrics.append(network_metric)
        for pid in process_manager.get_processes_pids():
            memory_metric = self.memory_collector.collect_metric(pid, index)
            if memory_metric is not None:
                self.memory_metrics.append(memory_metric)

            disk_metric = self.disk_collector.collect_metric(pid, index)
            if disk_metric is not None:
                self.disk_metrics.append(disk_metric)

            cpu_metric = self.cpu_collector.collect_metric(pid, index)
            if cpu_metric is not None:
                self.cpu_metrics.append(cpu_metric)

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

        return profiler

    def __repr__(self):
        return (
            f"Profiler(metrics={self.metrics}, function_timers={self.function_timers})"
        )

    def start_profiling(self, conn, parent_pid):
        index = 0
        while True:
            if conn.poll():  # This is non-blocking
                message = conn.recv()
                if message == "stop":
                    print("Received stop signal, stopping profiling.")
                    conn.send(self)
                    conn.close()
                    break
            self.metrics.collect_all_metrics(parent_pid, index)
            index += 1

    def update(self, received_data):
        if not isinstance(received_data, Profiler):
            raise ValueError("Received data is not an instance of Profiler")

        self.metrics.update(received_data.metrics)

    def to_dict(self):
        return {
            "metrics": self.metrics.to_dict(),
            "function_timers": [timer.to_dict() for timer in self.function_timers],
        }

    def to_json(self):
        return json.dumps(self.to_dict())
