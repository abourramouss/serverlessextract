import psutil
import time
import os
from multiprocessing import Process, Pipe
import contextlib
import json
from dataclasses import dataclass, asdict
from typing import Dict


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


@dataclass
class SystemMetric:
    timestamp: float
    pid: int
    metric_type: str
    value: Dict[str, float]

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        return cls(**data)


class IMetricCollector:
    def collect_metric(self, pid, metric_type):
        try:
            current_time = time.time()
            value = self._collect(pid)
            return SystemMetric(
                timestamp=current_time, pid=pid, metric_type=metric_type, value=value
            )
        except psutil.NoSuchProcess:
            print(f"Process with PID {pid} no longer exists.")
            return None

    def _collect(self, pid):
        raise NotImplementedError


class CPUMetricCollector(IMetricCollector):
    def __init__(self):
        pass

    def _collect(self, pid):
        try:
            return {"cpu_usage": psutil.Process(pid).cpu_percent(interval=0.5)}
        except psutil.NoSuchProcess:
            print(f"Process with PID {pid} no longer exists.")
            return None


class MemoryMetricCollector(IMetricCollector):
    def __init__(self):
        pass

    def _collect(self, pid):
        try:
            return psutil.Process(pid).memory_info().rss >> 20
        except psutil.NoSuchProcess:
            print(f"Process with PID {pid} no longer exists.")
            return None


class DiskMetricCollector(IMetricCollector):
    def _collect(self, pid):
        try:
            current_counter = psutil.Process(pid).io_counters()
            read = current_counter.read_bytes / 1024.0**2
            write = current_counter.write_bytes / 1024.0**2

            return {
                "disk_read_mb": read,
                "disk_write_mb": write,
            }
        except psutil.NoSuchProcess:
            print(f"Process with PID {pid} no longer exists.")
            return None


class NetworkMetricCollector(IMetricCollector):
    def _collect(self, pid):
        current_net_counters = psutil.net_io_counters(pernic=False)
        read_mb = current_net_counters.bytes_recv / 1024.0**2
        write_mb = current_net_counters.bytes_sent / 1024.0**2

        return {
            "net_read_mb": read_mb,
            "net_write_mb": write_mb,
        }


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
        self.all_metrics = {}

    @classmethod
    def from_dict(cls, data):
        instance = cls()
        for pid, metrics in data.items():
            instance.all_metrics[pid] = [
                SystemMetric.from_dict(metric) for metric in metrics
            ]
        return instance

    def __repr__(self):
        return f"MetricCollector(all_metrics={self.all_metrics})"

    def collect_all_metrics(self, parent_pid):
        process_manager = ProcessManager(parent_pid)
        print(f"tracking process pid {process_manager.get_processes_pids}")
        for pid in process_manager.get_processes_pids():
            if pid not in self.all_metrics:
                self.all_metrics[pid] = []
            self.all_metrics[pid].append(self.cpu_collector.collect_metric(pid, "cpu"))
            self.all_metrics[pid].append(
                self.memory_collector.collect_metric(pid, "memory")
            )
            self.all_metrics[pid].append(
                self.disk_collector.collect_metric(pid, "disk")
            )
            # Collect global network metrics and append them to the list
            network_metric = self.network_collector.collect_metric(
                "global_network", "network"
            )
            if "global_network" not in self.all_metrics:
                self.all_metrics["global_network"] = []
            self.all_metrics["global_network"].append(network_metric)

    def update(self, received_data):
        if not isinstance(received_data, MetricCollector):
            raise ValueError("Received data is not an instance of MetricCollector")

        # Update the MetricCollector data
        self.all_metrics.update(received_data.all_metrics)

    def to_dict(self):
        return {
            pid: [metric.to_dict() for metric in metrics]
            for pid, metrics in self.all_metrics.items()
        }


class Profiler:
    def __init__(
        self,
    ):
        self.metrics = MetricCollector()
        self.function_timers = []

    @classmethod
    def from_dict(cls, data):
        profiler = cls()
        profiler.metrics = MetricCollector.from_dict(data["metrics"])
        profiler.function_timers = [
            FunctionTimer.from_dict(timer) for timer in data["function_timers"]
        ]
        return profiler

    def __repr__(self):
        return f"{self.metrics.all_metrics},{self.function_timers}"

    def start_profiling(self, conn, parent_pid):
        while True:
            if conn.poll():  # This is non-blocking
                message = conn.recv()
                if message == "stop":
                    print("Received stop signal, stopping profiling.")
                    conn.send(self)
                    conn.close()
                    break
            self.metrics.collect_all_metrics(parent_pid)

    def update(self, received_data):
        if not isinstance(received_data, Profiler):
            raise ValueError("Received data is not an instance of Profiler")

        # Update the MetricCollector data
        self.metrics.update(received_data.metrics)

    def to_dict(self):
        return {
            "metrics": self.metrics.to_dict(),
            "function_timers": [timer.to_dict() for timer in self.function_timers],
        }

    def to_json(self):
        return json.dumps(self.to_dict())
