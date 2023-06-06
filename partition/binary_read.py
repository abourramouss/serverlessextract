import numpy as np

def read_measurement_set(filename):
    with open(filename, 'rb') as f:
        num_rows = np.fromfile(f, dtype=np.int32, count=1)[0]
        num_columns = np.fromfile(f, dtype=np.int32, count=1)[0]
        column_types = np.fromfile(f, dtype=np.int32, count=num_columns)

        table = []

        for _ in range(num_rows):
            row = []
            for col_type in column_types:
                if col_type == 0:  
                    data = np.fromfile(f, dtype=np.int32, count=1)[0]
                elif col_type == 1:  
                    data = np.fromfile(f, dtype=np.float64, count=1)[0]
                else:
                    raise ValueError(f"Unknown column type: {col_type}")
                row.append(data)
            table.append(row)

        return table
    

if __name__ == "__main__":
    read_measurement_set("/mnt/d/SB205.ms/table.f3_TSM0")