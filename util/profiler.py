import psutil
import time
import os
import socket
from multiprocessing import Process, Pipe
import contextlib
import json


def time_it(label, function, time_records, *args, **kwargs):
    print(f"label: {label}, type of function: {type(function)}")

    start_time = time.time()
    result = function(*args, **kwargs)
    end_time = time.time()

    record = {
        "label": label,
        "start_time": start_time,
        "end_time": end_time,
        "duration": (end_time - start_time),
    }
    time_records.append(record)

    return result


@contextlib.contextmanager
def profiling_context():
    parent_conn, child_conn = Pipe()
    profiler = Profiler()
    monitoring_process = Process(target=profiler.start_profiling, args=(child_conn,))
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


class ProcessManager:
    def __init__(self):
        self.pids = []

    def add_pid(self, pid):
        if pid not in self.pids:
            self.pids.append(pid)

    def get_all_children(self, parent_pid):
        children = []
        ignore_pid = os.getpid()
        for child in psutil.Process(parent_pid).children(recursive=True):
            if ignore_pid is not None and child.pid == ignore_pid:
                continue  # Skip the profiler process
            children.append(child.pid)
        return children

    def is_process_alive(self, pid):
        try:
            psutil.Process(pid)
            return True
        except psutil.NoSuchProcess:
            return False


class IMetricCollector:
    def collect_metrics(self):
        pass


class CPUMetricCollector(IMetricCollector):
    def collect_metrics(self, pid):
        return psutil.Process(pid).cpu_percent(interval=0.5)


class MemoryMetricCollector(IMetricCollector):
    def collect_metrics(self, pid):
        return psutil.Process(pid).memory_info().rss >> 20


class DiskMetricCollector(IMetricCollector):
    def collect_metrics(self, pid):
        current_counter = psutil.Process(pid).io_counters()
        read = current_counter.read_bytes / 1024.0**2
        write = current_counter.write_bytes / 1024.0**2

        return read, write


class NetworkMetricCollector(IMetricCollector):
    def collect_metrics(self):
        current_net_counters = psutil.net_io_counters(pernic=False)

        read = current_net_counters.bytes_recv / 1024.0**2
        write = current_net_counters.bytes_sent / 1024.0**2

        return read, write


class MetricCollector:
    def __init__(self, process_manager):
        self.process_manager = process_manager
        self.cpu_collector = CPUMetricCollector()
        self.memory_collector = MemoryMetricCollector()
        self.disk_collector = DiskMetricCollector()
        self.network_collector = NetworkMetricCollector()

    def collect_all_metrics(self):
        metrics = {}
        for pid in self.process_manager.pids:
            if self.process_manager.is_process_alive(pid):
                metrics[pid] = {
                    "cpu": self.cpu_collector.collect_metrics(pid),
                    "memory": self.memory_collector.collect_metrics(pid),
                    "disk": self.disk_collector.collect_metrics(pid),
                }
        metrics["network"] = self.network_collector.collect_metrics()
        return metrics


class Profiler:
    def __init__(self):
        # process managing
        self.pids = []  # List of children pids
        self.parent_pid = os.getpid()  # Parent who calls the profiling PID
        # statistics
        self.cpu_percent = []
        self.memory_used_mb = []
        self.disk_read = []
        self.disk_write = []
        self.net_write = []
        self.net_read = []
        self.time_records = []
        self.timestamps = []

    def __str__(self):
        return (
            f"CPU Percent: {self.cpu_percent}\n"
            f"Memory Used (MB): {self.memory_used_mb}\n"
            f"Disk Read Rate (MB/s): {self.disk_read}\n"
            f"Disk Write Rate (MB/s): {self.disk_write}\n"
            f"Bytes sent (MB/s): {self.net_write}\n"
            f"Bytes received (MB/s): {self.net_read}\n"
            f"Timerecords: {self.time_records}\n"
            f"Timestamps: {self.timestamps}\n"
        )

    def to_dict(self):
        return {
            "parent_pid": self.parent_pid,
            "cpu_percent": self.cpu_percent,
            "memory_used_mb": self.memory_used_mb,
            "disk_read": self.disk_read,
            "disk_write": self.disk_write,
            "net_write": self.net_write,
            "net_read": self.net_read,
            "pids": self.pids,
            "timestamps": self.timestamps,
        }

    @classmethod
    def from_dict(cls, data):
        profiler = cls()
        profiler.parent_pid = data["parent_pid"]
        profiler.cpu_percent = data["cpu_percent"]
        profiler.memory_used_mb = data["memory_used_mb"]
        profiler.disk_read = data["disk_read"]
        profiler.disk_write = data["disk_write"]
        profiler.net_write = data["net_write"]
        profiler.net_read = data["net_read"]
        profiler.pids = data["pids"]
        profiler.timestamps = data["timestamps"]

        return profiler

    def to_json(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        return cls.from_dict(data)

    def update(self, received_data):
        if not isinstance(received_data, Profiler):
            raise ValueError("Received data is not an instance of Profiler")

        # Merge the received data into the current profiler instance
        self.cpu_percent.extend(received_data.cpu_percent)
        self.memory_used_mb.extend(received_data.memory_used_mb)
        self.disk_read.extend(received_data.disk_read)
        self.disk_write.extend(received_data.disk_write)
        self.net_write.extend(received_data.net_write)
        self.net_read.extend(received_data.net_read)
        self.pids.extend(received_data.pids)
        self.timestamps.extend(received_data.timestamps)

    def add_pid(self, pid):
        if pid not in self.pids:
            self.pids.append(pid)

    def get_all_children(self, pid, ignore_pid=None):
        children = []
        for child in psutil.Process(pid).children(recursive=True):
            if ignore_pid is not None and child.pid == ignore_pid:
                continue  # Skip the profiler process
            children.append(child.pid)
        return children

    def start_profiling(self, conn):
        print(os.cpu_count())
        print(socket.gethostname())
        # NOTE: Start profiling starts as a new process and after the profiling
        # is done, it sends the profiler object back to the main process
        while True:
            # Check if there is a message in the pipe
            if conn.poll():  # This is non-blocking
                message = conn.recv()
                if message == "stop":
                    print("Received stop signal, stopping profiling.")
                    conn.send(self)
                    conn.close()
                    break

            self.pids = [self.parent_pid] + self.get_all_children(
                self.parent_pid, ignore_pid=os.getpid()
            )

            self.profile()
            # print(self)

        conn.close()

    def stop_profiling(self):
        # Set the event to signal that profiling should stop
        self._stop_profiling.set()

    def is_process_alive(self, pid):
        try:
            psutil.Process(pid)
            return True
        except psutil.NoSuchProcess:
            return False

    def profile(self):
        # Snapshot initial IO readings
        try:
            initial_disk_io = {
                pid: self.calculate_io_rate(pid)
                for pid in self.pids
                if self.is_process_alive(pid)
            }
            initial_net_io = self.calculate_net_rate()

            # Delay for CPU measurement - sum the CPU usage once per PID
            cpu_usage = sum(
                psutil.Process(pid).cpu_percent(interval=0.5)
                for pid in self.pids
                if self.is_process_alive(pid)
            )

            # Snapshot final IO readings
            final_disk_io = {
                pid: self.calculate_io_rate(pid)
                for pid in self.pids
                if self.is_process_alive(pid)
            }
            final_net_io = self.calculate_net_rate()

            # Calculate deltas for IO
            disk_read_rate = sum(
                final_disk_io[pid][0] - initial_disk_io[pid][0]
                for pid in self.pids
                if self.is_process_alive(pid)
            )
            disk_write_rate = sum(
                final_disk_io[pid][1] - initial_disk_io[pid][1]
                for pid in self.pids
                if self.is_process_alive(pid)
            )

            net_read_rate = final_net_io[0] - initial_net_io[0]
            net_write_rate = final_net_io[1] - initial_net_io[1]

            # Total memory usage
            total_mem = sum(
                self.get_memory_usage(pid)
                for pid in self.pids
                if self.is_process_alive(pid)
            )

            # Record the time and statistics
            current_time = time.time()
            self.timestamps.append(current_time)
            self.cpu_percent.append(cpu_usage)
            self.memory_used_mb.append(total_mem)
            self.disk_read.append(disk_read_rate)
            self.disk_write.append(disk_write_rate)
            self.net_read.append(net_read_rate)
            self.net_write.append(net_write_rate)
        except (psutil.NoSuchProcess, KeyError):
            # Process has ended or initial info was not captured; skip it
            pass

    def get_memory_usage(self, pid):
        return psutil.Process(pid).memory_info().rss >> 20

    def calculate_io_rate(self, pid):
        current_counter = psutil.Process(pid).io_counters()
        read = current_counter.read_bytes / 1024.0**2
        write = current_counter.write_bytes / 1024.0**2

        return read, write

    def calculate_net_rate(self):
        current_net_counters = psutil.net_io_counters(pernic=False)

        read = current_net_counters.bytes_recv / 1024.0**2
        write = current_net_counters.bytes_sent / 1024.0**2

        return read, write
