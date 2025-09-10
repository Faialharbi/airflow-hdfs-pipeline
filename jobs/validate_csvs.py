from pyspark.sql import SparkSession
import sys, glob

# >>> EDIT HERE if needed:
EXPECTED_COLUMNS = ["id", "name", "balance"]

def main():
    spark = (SparkSession.builder
             .appName("ValidateCSVs")
             .getOrCreate())

    # read all CSVs (HDFS or local)
    pattern = "<CSV_GLOB_PATH>"  
    files = spark._jsc.hadoopConfiguration().get("fs.defaultFS")
    # We simply let Spark read each file by path string
    # (We don't need to list via Java FS to keep it simple.)
    # Provide a small list of HDFS paths explicitly:
    paths = [pattern]  # Spark can expand this glob

    # Validate each file individually (Spark will expand the glob)
    # We read file by file using DataFrameReader on each match
    # Simple approach: just check columns (names/ordering)
    from pyspark.sql import functions as F

    # Collect matched files by using Spark to read the glob first, then inspect inputFiles()
    df_all = spark.read.option("header", True).csv(pattern)
    matched = list(set(df_all.inputFiles()))
    if not matched:
        print("[ERROR] No CSV files found under the provided pattern")
        spark.stop()
        sys.exit(3)

    ok = True
    for f in matched:
        df = spark.read.option("header", True).csv(f)
        cols = [c.strip() for c in df.columns]
        if cols != EXPECTED_COLUMNS:
            print(f"[ERROR] Schema mismatch for {f}")
            print(f"  expected: {EXPECTED_COLUMNS}")
            print(f"  actual  : {cols}")
            ok = False
        else:
            print(f"[OK] {f} schema is valid.")

    spark.stop()
    sys.exit(0 if ok else 4)

if _name_ == "_main_":
    main()
