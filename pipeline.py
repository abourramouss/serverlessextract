from steps.rebinning import RebinningStep
from steps.calibration import CalibrationStep, SubstractionStep, ApplyCalibrationStep
from steps.imaging import imaging, monitor_and_run_imaging
from s3path import S3Path
import logging
from util import setup_logging
import lithops

logger = logging.getLogger(__name__)
setup_logging(logging.INFO)


parameters = {
    "RebinningStep": {
        "input_data_path": S3Path("/ayman-extract/partitions/partitions_61zip"),
        "parameters": {
            "flagrebin": {
                "steps": "[aoflag, avg, count]",
                "aoflag.type": "aoflagger",
                "aoflag.memoryperc": 90,
                "aoflag.strategy": S3Path(
                    "/ayman-extract/parameters/rebinning/STEP1-NenuFAR64C1S.lua"
                ),
                "avg.type": "averager",
                "avg.freqstep": 4,
                "avg.timestep": 8,
            }
        },
        "output": S3Path("/ayman-extract/extract-data/rebinning_out/"),
    },
    "CalibrationStep": {
        "input_data_path": S3Path("/ayman-extract/extract-data/rebinning_out/"),
        "parameters": {
            "cal": {
                "msin": "",
                "msin.datacolumn": "DATA",
                "msout": ".",
                "steps": "[cal]",
                "cal.type": "ddecal",
                "cal.mode": "diagonal",
                "cal.sourcedb": S3Path(
                    "/ayman-extract/parameters/calibration/STEP2A-apparent.sourcedb"
                ),
                "cal.h5parm": "",
                "cal.solint": 4,
                "cal.nchan": 4,
                "cal.maxiter": 50,
                "cal.uvlambdamin": 5,
                "cal.smoothnessconstraint": 2e6,
            }
        },
        "output": S3Path("/ayman-extract/extract-data/calibration_out/"),
    },
    "SubstractionStep": {
        "input_data_path": S3Path("/ayman-extract/extract-data/calibration_out/"),
        "parameters": {
            "sub": {
                "msin": "",
                "msin.datacolumn": "DATA",
                "msout": ".",
                "msout.datacolumn": "SUBTRACTED_DATA",
                "steps": "[sub]",
                "sub.type": "h5parmpredict",
                "sub.sourcedb": S3Path(
                    "/ayman-extract/parameters/calibration/STEP2A-apparent.sourcedb"
                ),
                "sub.directions": "[[CygA],[CasA]]",
                "sub.operation": "subtract",
                "sub.applycal.parmdb": "",
                "sub.applycal.steps": "[sub_apply_amp,sub_apply_phase]",
                "sub.applycal.correction": "fulljones",
                "sub.applycal.sub_apply_amp.correction": "amplitude000",
                "sub.applycal.sub_apply_phase.correction": "phase000",
            }
        },
        "output": S3Path("/ayman-extract/extract-data/substraction_out/"),
    },
    "ApplyCalibrationStep": {
        "input_data_path": S3Path("/ayman-extract/extract-data/substraction_out/"),
        "parameters": {
            "apply": {
                "msin": "",
                "msin.datacolumn": "SUBTRACTED_DATA",
                "msout": ".",
                "msout.datacolumn": "CORRECTED_DATA",
                "steps": "[apply]",
                "apply.type": "applycal",
                "apply.steps": "[apply_amp,apply_phase]",
                "apply.apply_amp.correction": "amplitude000",
                "apply.apply_phase.correction": "phase000",
                "apply.direction": "[Main]",
                "apply.parmdb": "",
            }
        },
        "output": S3Path("/ayman-extract/extract-data/applycal_out/"),
    },
    "ImagingStep": {
        "input_data_path": S3Path("/ayman-extract/extract-data/applycal_out/ms/"),
        "output_path": S3Path("/ayman-extract/extract-data/imaging_out/"),
    },
}
"""

runtime_memory = [1769, 3538, 5307, 7076, 8845, 10000]
for i, p in enumerate([7]):
    for e in runtime_memory[0 : len(runtime_memory) - i]:
        print(p, e)
        rebinning_profilers = RebinningStep(
            input_data_path=S3Path(f"/ayman-extract/partitions/partitions_{p}zip"),
            parameters=parameters["RebinningStep"]["parameters"],
            output=parameters["RebinningStep"]["output"],
        ).run(1, e)
        path = f"plots/reb/{7900//p}mb_{e}"
        ProfilerPlotter.plot_average_profiler(rebinning_profilers, path)
        ProfilerPlotter.plot_aggregated_profiler(rebinning_profilers, path)
        ProfilerPlotter.plot_aggregated_sum_profiler(rebinning_profilers, path)
        ProfilerPlotter.plot_gantt(rebinning_profilers, path)
        with open(f"{path}/profiler.txt", "w") as f:
            f.write(str(rebinning_profilers[0]))



"""
rebinning_profilers = RebinningStep(
    input_data_path=S3Path(parameters["RebinningStep"]["input_data_path"]),
    parameters=parameters["RebinningStep"]["parameters"],
    output=parameters["RebinningStep"]["output"],
).run(1)

calibration_profilers = CalibrationStep(
    input_data_path=parameters["CalibrationStep"]["input_data_path"],
    parameters=parameters["CalibrationStep"]["parameters"],
    output=parameters["CalibrationStep"]["output"],
).run(1)

SubstractionStep(
    input_data_path=parameters["SubstractionStep"]["input_data_path"],
    parameters=parameters["SubstractionStep"]["parameters"],
    output=parameters["SubstractionStep"]["output"],
).run(1)


ApplyCalibrationStep(
    input_data_path=parameters["ApplyCalibrationStep"]["input_data_path"],
    parameters=parameters["ApplyCalibrationStep"]["parameters"],
    output=parameters["ApplyCalibrationStep"]["output"],
).run(1)


fexec = lithops.FunctionExecutor()

fexec.call_async(
    monitor_and_run_imaging,
    parameters["ImagingStep"],
    timeout=1800,
)
fexec.get_result()
"""




ProfilerPlotter.plot_average_profiler(calibration_profilers, f"plots/cal")
ProfilerPlotter.plot_aggregated_profiler(calibration_profilers, f"plots/cal")
ProfilerPlotter.plot_aggregated_sum_profiler(calibration_profilers, f"plots/cal")
ProfilerPlotter.plot_gantt(calibration_profilers, "plots/cal")





ApplyCalibrationStep(
    input_data_path=parameters["ApplyCalibrationStep"]["input_data_path"],
    parameters=parameters["ApplyCalibrationStep"]["parameters"],
    output=parameters["ApplyCalibrationStep"]["output"],
).run()



"""
