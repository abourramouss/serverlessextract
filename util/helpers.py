import os
import shutil
from pathlib import PosixPath
from s3path import S3Path
rebinning_param_parset = {
    "msin": "",
    "msout": "",
    "steps": "[aoflag, avg, count]",
    "aoflag.type": "aoflagger",
    "aoflag.memoryperc": 90,
    "aoflag.strategy": "/home/ayman/Downloads/pipeline/parameters/rebinning/STEP1-NenuFAR64C1S.lua",
    "avg.type": "averager",
    "avg.freqstep": 4,
    "avg.timestep": 8,
}

cal_param_parset = {
    "msin": "",
    "msin.datacolumn": "DATA",
    "msout": ".",
    "steps": "[cal]",
    "cal.type": "ddecal",
    "cal.mode": "diagonal",
    "cal.sourcedb": "/home/ayman/Downloads/pipeline/parameters/cal/STEP2A-apparent.sourcedb",
    "cal.h5parm": "",
    "cal.solint": 4,
    "cal.nchan": 4,
    "cal.maxiter": 50,
    "cal.uvlambdamin": 5,
    "cal.smoothnessconstraint": 2e6,
}

sub_param_parset = {
    "msin": "",
    "msin.datacolumn": "DATA",
    "msout": ".",
    "msout.datacolumn": "SUBTRACTED_DATA",
    "steps": "[sub]",
    "sub.type": "h5parmpredict",
    "sub.sourcedb": "/home/ayman/Downloads/pipeline/parameters/cal/STEP2A-apparent.sourcedb",
    "sub.directions": "[[CygA], [CasA]]",
    "sub.operation": "subtract",
    "sub.applycal.parmdb": "",
    "sub.applycal.steps": "[sub_apply_amp, sub_apply_phase]",
    "sub.applycal.correction": "fulljones",
    "sub.applycal.sub_apply_amp.correction": "amplitude000",
    "sub.applycal.sub_apply_phase.correction": "phase000",
}

apply_cal_param_parset = {
    "msin": "",
    "msin.datacolumn": "SUBTRACTED_DATA",
    "msout": ".",
    "msout.datacolumn": "CORRECTED_DATA",
    "steps": "[apply]",
    "apply.type": "applycal",
    "apply.steps": "[apply_amp, apply_phase]",
    "apply.apply_amp.correction": "amplitude000",
    "apply.apply_phase.correction": "phase000",
    "apply.direction": "[Main]",
    "apply.parmdb": "",
}


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
