import psutil
import time
import os
import socket


class Profiler:
    def __init__(self, pid=None):
        self.cpu_percent = []
        self.memory_used_mb = []
        self.disk_read_rate_mb = []
        self.disk_write_rate_mb = []
        self.bytes_sent = []
        self.bytes_recv = []
        self._stop_profiling = False
        self.pid = pid or os.getpid()
        self.pids = [self.pid]
        self.prev_io_counters = psutil.disk_io_counters()
        self.time_records = []
        self.prev_net_counters = psutil.net_io_counters(pernic=False)
        self.timestamps = []

    def __str__(self):
        return (
            f"CPU Percent: {self.cpu_percent}\n"
            f"Memory Used (MB): {self.memory_used_mb}\n"
            f"Disk Read Rate (MB/s): {self.disk_read_rate_mb}\n"
            f"Disk Write Rate (MB/s): {self.disk_write_rate_mb}\n"
            f"Bytes sent (MB/s): {self.bytes_sent}\n"
            f"Bytes received (MB/s): {self.bytes_recv}\n"
            f"Timestamps: {self.timestamps}\n"
        )

    @property
    def process(self):
        return psutil.Process(self.pid)

    def add_pid(self, pid):
        if pid not in self.pids:
            self.pids.append(pid)

    def get_all_children(self, pid):
        children = []
        for child in psutil.Process(pid).children(recursive=True):
            children.append(child.pid)
        return children

    def start_profiling(self):
        print(os.cpu_count())
        print(socket.gethostname())

        while not self._stop_profiling:
            self.pids = [self.pid] + self.get_all_children(self.pid)
            print(f"Profiling PIDs: {self.pids}")
            self.profile()

            if not self.is_process_alive(self.pid):
                print(
                    f"Parent process with PID {self.pid} no longer exists. Stopping profiling."
                )
                self._stop_profiling = True
                break

    def is_process_alive(self, pid):
        try:
            psutil.Process(pid)
            return True
        except psutil.NoSuchProcess:
            return False

    def profile(self, duration=1):
        total_cpu = 0
        total_mem = 0
        current_time = time.time()

        read_rate, write_rate = self.calculate_io_rate(duration)
        bytes_recv, bytes_sent = self.calculate_net_rate(duration)

        for pid in self.pids:
            try:
                process = psutil.Process(pid)
                total_cpu += process.cpu_percent(duration)
                total_mem += self.get_memory_usage(pid)
            except psutil.NoSuchProcess:
                # Process does not exist anymore, but we simply skip and continue
                pass

        # Append stats only if the parent process is still alive
        if self.is_process_alive(self.pid):
            self.timestamps.append(current_time)
            self.cpu_percent.append(total_cpu)
            self.memory_used_mb.append(total_mem)
            self.disk_read_rate_mb.append(read_rate)
            self.disk_write_rate_mb.append(write_rate)
            self.bytes_recv.append(bytes_recv)
            self.bytes_sent.append(bytes_sent)

    def get_memory_usage(self, pid):
        return psutil.Process(pid).memory_info().rss >> 20

    def calculate_io_rate(self, duration):
        current_io_counters = psutil.disk_io_counters()

        read_rate = (
            current_io_counters.read_bytes - self.prev_io_counters.read_bytes
        ) / (1024.0**2 * duration)
        write_rate = (
            current_io_counters.write_bytes - self.prev_io_counters.write_bytes
        ) / (1024.0**2 * duration)

        self.prev_io_counters = current_io_counters
        return read_rate, write_rate

    def calculate_net_rate(self, duration):
        current_net_counters = psutil.net_io_counters(pernic=False)

        bytes_recv = (
            current_net_counters.bytes_recv - self.prev_net_counters.bytes_recv
        ) / (1024.0**2 * duration)
        bytes_sent = (
            current_net_counters.bytes_sent - self.prev_net_counters.bytes_sent
        ) / (1024.0**2 * duration)

        self.prev_net_counters = current_net_counters
        return bytes_recv, bytes_sent

    def stop_profiling(self):
        self._stop_profiling = True

    def time_it(self, label, function, *args, **kwargs):
        start_time = time.time()
        result = function(*args, **kwargs)
        end_time = time.time()

        record = {
            "label": label,
            "start_time": start_time,
            "end_time": end_time,
            "duration": (end_time - start_time),
        }
        self.time_records.append(record)
        print(self.time_records)

        return result
