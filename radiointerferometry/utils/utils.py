import os
import subprocess as sp
from pathlib import PosixPath
import logging
import requests
from lithops.utils import get_executor_id


def detect_runtime_environment():
    if "AWS_LAMBDA_FUNCTION_NAME" in os.environ:
        return ("AWS Lambda", None)

    try:
        token_response = requests.put(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
            timeout=1,
        )
        if token_response.status_code == 200:
            token = token_response.text
            response = requests.get(
                "http://169.254.169.254/latest/meta-data/instance-type",
                headers={"X-aws-ec2-metadata-token": token},
                timeout=1,
            )
            if response.status_code == 200:
                return ("Amazon EC2", response.text)
    except requests.exceptions.RequestException:
        pass

    if "KUBERNETES_SERVICE_HOST" in os.environ:
        return ("Kubernetes", None)

    return ("Unknown", None)


def get_memory_limit_cgroupv2():
    try:
        output = (
            sp.check_output(["cat", "/sys/fs/cgroup/memory.max"])
            .decode("utf-8")
            .strip()
        )
        if output == "max":
            return "No limit"
        memory_limit_gb = int(output) / (1024**3)
        return memory_limit_gb
    except Exception as e:
        return str(e)


def get_cpu_limit_cgroupv2():
    try:
        with open("/sys/fs/cgroup/cpu.max") as f:
            cpu_max = f.read().strip()
            quota, period = cpu_max.split(" ")
            quota = int(quota)
            period = int(period)

        if quota == -1:  # No limit
            return "No limit"
        else:
            cpu_limit = quota / period
            return cpu_limit
    except Exception as e:
        return str(e)


def get_dir_size(start_path="."):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def dict_to_parset(
    data, output_dir=PosixPath("/tmp"), filename="output.parset"
) -> PosixPath:
    lines = []

    for key, value in data.items():
        # Check if the value is another dictionary
        if isinstance(value, dict):
            lines.append(f"[{key}]")
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, dict):  # Check for nested dictionaries
                    lines.append(f"[{sub_key}]")
                    for sub_sub_key, sub_sub_value in sub_value.items():
                        lines.append(f"{sub_sub_key} = {sub_sub_value}")
                else:
                    lines.append(f"{sub_key} = {sub_value}")
        else:
            lines.append(f"{key} = {value}")

    # Convert the list of lines to a single string
    parset_content = "\n".join(lines)

    # Ensure the directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / filename
    with output_path.open("w") as file:
        file.write(parset_content)

    return output_path


def setup_logging(level):
    logger = logging.getLogger(__name__)
    logger.handlers.clear()
    logger.setLevel(level)
    logger.propagate = False

    log_format = (
        "%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d -- %(message)s"
        if level == logging.DEBUG
        else "%(asctime)s [%(levelname)s] %(message)s"
    )
    handler = logging.StreamHandler()
    handler.setLevel(level)
    formatter = logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def get_executor_id_lithops():
    lithops_exec_id = get_executor_id().split("-")[0]
    return lithops_exec_id
