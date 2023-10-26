import os
import shutil
from pathlib import PosixPath
import logging
import sys


def delete_all_in_cwd():
    cwd = os.getcwd()
    for filename in os.listdir(cwd):
        try:
            if os.path.isfile(filename) or os.path.islink(filename):
                os.unlink(filename)
            elif os.path.isdir(filename):
                shutil.rmtree(filename)
        except Exception as e:
            print(f"Failed to delete {filename}. Reason: {e}")


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


def setup_logging(level=logging.INFO):
    interferometry_logger = logging.getLogger()
    interferometry_logger.propagate = False

    interferometry_logger.setLevel(level)
    sh = logging.StreamHandler(stream=sys.stdout)
    sh.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s:%(lineno)s - %(message)s"
    )
    sh.setFormatter(formatter)
    interferometry_logger.addHandler(sh)

    # Format Lithops logger the same way as serverlessgenomics module logger
    lithops_logger = logging.getLogger("lithops")
    lithops_logger.propagate = False

    lithops_logger.setLevel(level)
    sh = logging.StreamHandler(stream=sys.stdout)
    sh.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh.setFormatter(formatter)
    lithops_logger.addHandler(sh)

    # Disable module analyzer logger from Lithops
    multyvac_logger = logging.getLogger("lithops.libs.multyvac")
    multyvac_logger.setLevel(logging.CRITICAL)
