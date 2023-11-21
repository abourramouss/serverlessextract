import psutil
import time
import os
import socket
from multiprocessing import Process, Pipe
import contextlib


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
    profiler = Profiler(os.getpid())
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


class Profiler:
    def __init__(self, pid):
        self.cpu_percent = []
        self.memory_used_mb = []
        self.disk_read = []
        self.disk_write = []
        self.net_write = []
        self.net_read = []
        self.pids = []
        self.parent_pid = pid
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
            f"Timestamps: {self.timestamps}\n"
        )

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
