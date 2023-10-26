import matplotlib.pyplot as plt
import numpy as np
import os

from .profiler import Profiler


class ProfilerPlotter:
    @classmethod
    def average(cls, profilers):
        """
        Given a list of Profiler objects, create a new Profiler with averaged data.
        """
        avg_profiler = Profiler()
        metrics = [
            "cpu_percent",
            "memory_used_mb",
            "disk_read_rate_mb",
            "disk_write_rate_mb",
            "upload_rate_mb",  # Added this
            "download_rate_mb",  # Added this
        ]

        for metric in metrics:
            avg_data = cls.average_metric([getattr(prof, metric) for prof in profilers])
            setattr(avg_profiler, metric, avg_data)

        return avg_profiler

    @classmethod
    def plot(cls, profiler, title="Profiler Plot", filename="plot.png"):
        """
        Given a Profiler object, plot the CPU usage, Memory usage, Disk read, Disk write, Upload rate, and Download rate over time.
        Save the plot to the specified filename within the "plots" directory.
        """

        print(profiler.cpu_percent)
        print(profiler.memory_used_mb)
        print(profiler.disk_read_rate_mb)
        print(profiler.disk_write_rate_mb)
        print(profiler.upload_rate_mb)
        print(profiler.download_rate_mb)

        time = np.arange(len(profiler.cpu_percent))

        fig, ax = plt.subplots(3, 2, figsize=(14, 15))  # Updated to 3x2 grid

        ax[0, 0].plot(time, profiler.cpu_percent, label="CPU Usage (%)", color="blue")
        ax[0, 0].set_title("CPU Usage Over Time")
        ax[0, 0].set_xlabel("Time")
        ax[0, 0].set_ylabel("Percentage")
        ax[0, 0].legend()

        ax[0, 1].plot(
            time, profiler.memory_used_mb, label="Memory Usage (MB)", color="green"
        )
        ax[0, 1].set_title("Memory Usage Over Time")
        ax[0, 1].set_xlabel("Time")
        ax[0, 1].set_ylabel("Memory (MB)")
        ax[0, 1].legend()

        ax[1, 0].plot(
            time, profiler.disk_read_rate_mb, label="Disk Read (MB)", color="red"
        )
        ax[1, 0].set_title("Disk Read Over Time")
        ax[1, 0].set_xlabel("Time")
        ax[1, 0].set_ylabel("Disk Read (MB)")
        ax[1, 0].legend()

        ax[1, 1].plot(
            time, profiler.disk_write_rate_mb, label="Disk Write (MB)", color="purple"
        )
        ax[1, 1].set_title("Disk Write Over Time")
        ax[1, 1].set_xlabel("Time")
        ax[1, 1].set_ylabel("Disk Write (MB)")
        ax[1, 1].legend()

        # Added plots for network throughput
        ax[2, 0].plot(
            time, profiler.upload_rate_mb, label="Upload Rate (MB/s)", color="cyan"
        )
        ax[2, 0].set_title("Upload Rate Over Time")
        ax[2, 0].set_xlabel("Time")
        ax[2, 0].set_ylabel("Upload Rate (MB/s)")
        ax[2, 0].legend()

        ax[2, 1].plot(
            time,
            profiler.download_rate_mb,
            label="Download Rate (MB/s)",
            color="orange",
        )
        ax[2, 1].set_title("Download Rate Over Time")
        ax[2, 1].set_xlabel("Time")
        ax[2, 1].set_ylabel("Download Rate (MB/s)")
        ax[2, 1].legend()

        plt.suptitle(title, fontsize=16)
        plt.tight_layout()
        plt.subplots_adjust(top=0.95)  # Adjusted for the new row

        # Ensure the 'plots' directory exists
        if not os.path.exists("plots"):
            os.makedirs("plots")

        # Save the figure
        plt.savefig(os.path.join("plots", filename))
        plt.close()

    @staticmethod
    def average_metric(lists):
        """
        Given a list of lists, calculate the average of each position.
        """
        return [sum(values) / len(values) for values in zip(*lists)]
