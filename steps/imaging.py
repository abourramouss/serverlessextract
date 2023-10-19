from typing import List
from s3path import S3Path
from datasource import LithopsDataSource
import os


def imaging(calibrated_ms: S3Path, images_path: S3Path) -> List[str]:
    data_source = LithopsDataSource()
    cal_partition_path = data_source.download_directory(calibrated_ms)

    cal_ms = [
        d
        for d in os.listdir(cal_partition_path)
        if os.path.isdir(os.path.join(cal_partition_path, d))
    ]

    print("cal ms", cal_ms)

    """
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
        os.path.join(output_dir, self.output_name),
    ]

    print("command", cmd)

    proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
    stdout, stderr = proc.communicate()
    print(stdout, stderr)

    data_source.upload_directory(
        cal_partition_path, S3Path(f"{substracted_ms}/ms/{output_ms}")
    )
    
    """
