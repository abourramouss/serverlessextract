import subprocess
from .step import Step
from datasource import LithopsDataSource
import os
class CalibrationStep(Step):
    def __init__(self, calibration_file, skymodel_file, sourcedb_file):
        self.skymodel_file = skymodel_file
        self.sourcedb_file = sourcedb_file
        self.calibration_file = calibration_file
        
    def run(self, calibrated_mesurement_set: str, bucket_name: str, output_dir: str) -> None:
        
        self.datasource = LithopsDataSource()
        
        os.chdir(output_dir)
        #Download rebined mesurement set
        calibrated_name = calibrated_mesurement_set.split('/')[-1]
        calibrated_name = calibrated_name.split('.')[0]
        output_h5 = f"{calibrated_name}.h5"
        self.datasource.download(bucket_name, calibrated_mesurement_set, output_dir)
       
        #Download the parameter folder for DP3
        self.datasource.download(bucket_name, f"extract-data/parameters", output_dir)
        curr_dir = os.getcwd()
        os.chdir(f'extract-data/parameters')
        for file in os.listdir():
            print(file)
        
        os.chdir(curr_dir)
        
        os.makedirs('DATAREB', exist_ok=True)
            
        cmd = [
                "DP3",
                self.calibration_file,
                f"msin={calibrated_mesurement_set}",
                f"cal.h5parm=DATAREB/{output_h5}",
                f"cal.sourcedb={self.sourcedb_file}"
            ]
        
        
        print(cmd)
                
        out = subprocess.run(cmd, capture_output=True, text=True)
        print(out.stdout)
        print(out.stderr)
        
        self.datasource.storage.upload_file(f'/tmp/DATAREB/{output_h5}', bucket_name, f'extract-data/step2a_out/{output_h5}')

        return f'extract-data/step2a_out/{output_h5}'
    

    

class SubtractionStep(Step):
    def __init__(self, calibration_file, sourcedb_file):
        
        self.calibration_file = calibration_file
        self.source_db = sourcedb_file

    def run(self, calibrated_mesurement_set: str, bucket_name: str, output_dir: str):
        
        self.datasource = LithopsDataSource()
        os.chdir(output_dir)

        calibrated_name = calibrated_mesurement_set.split('/')[-1]
        calibrated_name = calibrated_name.split('.')[0]
        output_h5 = f"{calibrated_name}.h5"
        
        self.datasource.download(bucket_name, calibrated_mesurement_set, output_dir)
        self.datasource.download(bucket_name, f"extract-data/parameters", output_dir)
        
        self.datasource.storage.download_file(bucket_name, f'extract-data/step2a_out/{output_h5}', f'/tmp/DATAREB/{output_h5}')
        
        
        cmd = [
            "DP3",
            self.calibration_file,
            f"msin={calibrated_mesurement_set}",
            f"sub.applycal.parmdb=DATAREB/{output_h5}",
            f"sub.sourcedb={self.source_db}",
        ]
        
        
        out = subprocess.run(cmd, capture_output=True, text=True)
        
        print(out.stdout)
        print(out.stderr)

        self.datasource.upload(bucket_name, 'extract-data/step2b_out', f'/tmp/{calibrated_mesurement_set}')
        
        return f'extract-data/step2b_out/{calibrated_name}.ms'
        


class ApplyCalibrationStep(Step):
    def __init__(self, calibration_file):
        
        self.calibration_file = calibration_file

    def run(self, calibrated_mesurement_set: str, bucket_name: str, output_dir: str):
        
        self.datasource = LithopsDataSource()
        os.chdir(output_dir)

        calibrated_name = calibrated_mesurement_set.split('/')[-1]
        calibrated_name = calibrated_name.split('.')[0]
        output_h5 = f"{calibrated_name}.h5"
        
        self.datasource.download(bucket_name, calibrated_mesurement_set, output_dir)
        self.datasource.download(bucket_name, f"extract-data/parameters", output_dir)
        
        self.datasource.storage.download_file(bucket_name, f'extract-data/step2a_out/{output_h5}', f'/tmp/DATAREB/{output_h5}')
        
        print(calibrated_mesurement_set)
        cmd = [
            "DP3",
            self.calibration_file,
            f"msin={calibrated_mesurement_set}",
            f"apply.parmdb=DATAREB/{output_h5}"
        ]
        
        out = subprocess.run(cmd, capture_output=True, text=True)
        
        print(out.stdout)
        print(out.stderr)

        self.datasource.upload(bucket_name, 'extract-data/step2c_out', f'/tmp/{calibrated_mesurement_set}')
        
        return f'extract-data/step2c_out/{calibrated_name}.ms'