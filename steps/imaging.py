from s3path import S3Path
import subprocess
import os
import sys
import cProfile
import psutil
import matplotlib.pyplot as plt
from memory_profiler import profile
import time
from pathlib import PosixPath
from datasource import LithopsDataSource


class Profiler:
    def __init__(self):
        self.cpu_percent = []
        self.memory_percent = []
        self.disk_percent = []

    def profile(self, duration=1):
        self.cpu_percent.append(psutil.cpu_percent())
        self.memory_percent.append(psutil.virtual_memory().percent)
        self.disk_percent.append(psutil.disk_usage("/").percent)
        time.sleep(duration)

    def plot(self, plot_path):
        plt.figure(figsize=(10, 7))

        plt.plot(self.cpu_percent, label="CPU Usage (%)", color="red")
        plt.plot(self.memory_percent, label="Memory Usage (%)", color="blue")
        plt.plot(self.disk_percent, label="Disk Usage (%)", color="green")

        plt.xlabel("Time (seconds)")
        plt.ylabel("Usage (%)")
        plt.title("Resource Usage Over Time")
        plt.legend()

        plt.savefig(plot_path)


class Monitor:
    def __init__(self):
        self.profiler = Profiler()
        self.datasource = LithopsDataSource()

    def monitor_resources(self, duration):
        end_time = time.time() + duration
        while time.time() < end_time:
            self.profiler.profile()

    def start(self, cmd, output_path):
        process = subprocess.Popen(cmd)
        self.monitor_resources(duration=process.runtime)
        self.profiler.plot("/tmp/usage_plot.png")
        self.upload_file(PosixPath("/tmp/usage_plot.png"), output_path)

    def upload_file(self, read_path: PosixPath, write_path: S3Path) -> None:
        try:
            self.datasource.upload_file(str(read_path), write_path)
        except Exception as e:
            print(f"Failed to upload file {read_path} to {write_path}. Error: {e}")


def run_command_and_stream_output(command):
    # Start the process with pipes enabled for stdout and stderr
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    # Continuously read from the pipes and print to the console
    while True:
        # Read one line from stdout
        output = process.stdout.readline()

        # If the subprocess has terminated, break the loop
        if output == "" and process.poll() is not None:
            break

        # Print the output line
        if output:
            sys.stdout.write(output)
            sys.stdout.flush()

    # Once the process is done, get any remaining lines from stdout/stderr
    _, errs = process.communicate()

    # Print the remaining stderr
    if errs:
        sys.stderr.write(errs)


@profile
def imaging(input_data_path: S3Path, output_path: S3Path):
    data_source = LithopsDataSource()
    cal_partition_path = data_source.download_directory(input_data_path)

    cal_ms = [
        d
        for d in os.listdir(cal_partition_path)
        if os.path.isdir(os.path.join(cal_partition_path, d))
    ]

    os.chdir(cal_partition_path)

    cmd = [
        "wsclean",
        "-size",
        "1024",
        "1024",
        "-pol",
        "I",
        "-scale",
        "5arcmin",
        "-niter",
        "100000",
        "-gain",
        "0.1",
        "-mgain",
        "0.6",
        "-auto-mask",
        "5",
        "-local-rms",
        "-multiscale",
        "-no-update-model-required",
        "-make-psf",
        "-auto-threshold",
        "3",
        "-weight",
        "briggs",
        "0",
        "-data-column",
        "CORRECTED_DATA",
        "-nmiter",
        "0",
        "-name",
        "/tmp/Cygloop-205-210-b0-1024",
    ]

    directories = [d for d in os.listdir(".") if os.path.isdir(d)]
    cmd.extend(directories)
    print("command", cmd)

    run_command_and_stream_output(cmd)


def monitor_and_run_imaging(
    input_data_path,
    output_path,
):
    monitor = Monitor()

    pr = cProfile.Profile()
    pr.enable()

    # Disk usage before
    du_before = psutil.disk_io_counters()

    imaging(input_data_path, output_path)

    # Disk usage after
    du_after = psutil.disk_io_counters()

    pr.disable()
    pr.dump_stats("cpu_stats.prof")

    # Calculate the disk reads/writes
    read_bytes = du_after.read_bytes - du_before.read_bytes
    write_bytes = du_after.write_bytes - du_before.write_bytes

    # Visualization of Disk Usage
    plt.figure()
    plt.bar(["Read", "Write"], [read_bytes, write_bytes])
    plt.ylabel("Bytes")
    plt.title("Disk Read/Write during execution")
    plt_path = "/tmp/disk_usage_plot.png"
    plt.savefig(plt_path)
    plt.show()

    # Upload the disk usage plot to S3
    monitor.upload_file(PosixPath(plt_path), output_path)
