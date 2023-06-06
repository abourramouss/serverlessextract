import casacore.tables as tables

def inspect_ms_metadata(measurement_set_path):

    with tables.table(measurement_set_path, readonly=True, ack=False) as t:
        print("Table information:")
        print(t.info())

        print("\nColumn names:")
        colnames = t.colnames()
        print(colnames)

        print("\nTable keywords:")
        for keyword, value in t.getkeywords().items():
            print(f"{keyword}: {value}")

        print("\nNumber of rows:")
        print(t.nrows())

        print("\nFirst 5 rows of the table:")
        for i in range(min(5, t.nrows())):
            print(f"Row {i}:")
            for colname in colnames:
                try:
                    print(f"  {colname}: {t.getcell(colname, i)}")
                except RuntimeError as e:
                    print(f"  {colname}: Error retrieving value - {e}")


if __name__ == "__main__":
    measurement_set_path = "/mnt/d/SB205.ms"  
    inspect_ms_metadata(measurement_set_path)
    
    inspect_ms_metadata("/mnt/d/SB205.MS/ANTENNA")