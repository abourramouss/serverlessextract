import psutil
import time
import os


class Profiler:
    def __init__(self, pid=None):
        self.cpu_percent = []
        self.memory_used_mb = []
        self.disk_read_rate_mb = []
        self.disk_write_rate_mb = []
        self.upload_rate_mb = []
        self.download_rate_mb = []
        self._stop_profiling = False
        self.pid = pid or os.getpid()
        self.pids = [self.pid]
        self.prev_io_counters = {}
        self.prev_net_counters = None

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
        while not self._stop_profiling:
            if not self.is_process_alive(self.pid):
                print(
                    f"Parent process with PID {self.pid} no longer exists. Stopping profiling."
                )
                self._stop_profiling = True
                break
            self.pids = [self.pid] + self.get_all_children(self.pid)
            print(f"Profiling PIDs: {self.pids}")
            self.profile()
            time.sleep(0.5)

    def is_process_alive(self, pid):
        try:
            psutil.Process(pid)
            return True
        except psutil.NoSuchProcess:
            return False

    def profile(self):
        total_cpu = 0
        total_mem = 0
        total_read_rate = 0
        total_write_rate = 0

        for pid in self.pids:
            if self.is_process_alive(pid):
                process = psutil.Process(pid)
                total_cpu += process.cpu_percent(interval=0.5)
                total_mem += self.get_memory_usage(pid)
                read_rate, write_rate = self.calculate_io_rate(pid)
                total_read_rate += read_rate
                total_write_rate += write_rate

        upload_rate, download_rate = self.calculate_net_rate()
        self.cpu_percent.append(total_cpu)
        self.memory_used_mb.append(total_mem)
        self.disk_read_rate_mb.append(
            total_read_rate
        )  # Already in MB/s because of the 1-second interval
        self.disk_write_rate_mb.append(
            total_write_rate
        )  # Already in MB/s because of the 1-second interval
        self.upload_rate_mb.append(
            upload_rate
        )  # Already in MB/s because of the 1-second interval
        self.download_rate_mb.append(
            download_rate
        )  # Already in MB/s because of the 1-second interval

    def get_memory_usage(self, pid):
        return psutil.Process(pid).memory_info().rss / (1024**2)

    def calculate_io_rate(self, pid):
        process = psutil.Process(pid)
        io_counters = process.io_counters()

        if pid not in self.prev_io_counters:
            self.prev_io_counters[pid] = io_counters
            return 0, 0

        read_rate = (
            io_counters.read_bytes - self.prev_io_counters[pid].read_bytes
        ) / 1024.0**2
        write_rate = (
            io_counters.write_bytes - self.prev_io_counters[pid].write_bytes
        ) / 1024.0**2
        self.prev_io_counters[pid] = io_counters

        return read_rate, write_rate

    def calculate_net_rate(self):
        net_counters = psutil.net_io_counters()
        if self.prev_net_counters is None:
            self.prev_net_counters = net_counters
            return 0, 0
        download_rate = (
            net_counters.bytes_recv - self.prev_net_counters.bytes_recv
        ) / 1024.0**2
        upload_rate = (
            net_counters.bytes_sent - self.prev_net_counters.bytes_sent
        ) / 1024.0**2
        self.prev_net_counters = net_counters
        return upload_rate, download_rate

    def stop_profiling(self):
        self._stop_profiling = True

    def __str__(self):
        return (
            f"CPU Percent: {self.cpu_percent}\n"
            f"Memory Used (MB): {self.memory_used_mb}\n"
            f"Disk Read Rate (MB/s): {self.disk_read_rate_mb}\n"
            f"Disk Write Rate (MB/s): {self.disk_write_rate_mb}\n"
            f"Upload Rate (MB/s): {self.upload_rate_mb}\n"
            f"Download Rate (MB/s): {self.download_rate_mb}\n"
        )
