
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
import os


# def define_schema():
    
#     schema = StructType([
#         StructField(" Destination Port", IntegerType(), True),
#         StructField(" Flow Duration", LongType(), True),
#         StructField(" Total Fwd Packets", IntegerType(), True),
#         StructField(" Total Backward Packets", IntegerType(), True),
#         StructField("Total Length of Fwd Packets", DoubleType(), True),
#         StructField(" Total Length of Bwd Packets", DoubleType(), True),
#         StructField(" Fwd Packet Length Max", DoubleType(), True),
#         StructField(" Fwd Packet Length Min", DoubleType(), True),
#         StructField(" Fwd Packet Length Mean", DoubleType(), True),
#         StructField(" Fwd Packet Length Std", DoubleType(), True),
#         StructField("Bwd Packet Length Max", DoubleType(), True),
#         StructField(" Bwd Packet Length Min", DoubleType(), True),
#         StructField(" Bwd Packet Length Mean", DoubleType(), True),
#         StructField(" Bwd Packet Length Std", DoubleType(), True),
#         StructField("Flow Bytes/s", DoubleType(), True),
#         StructField(" Flow Packets/s", DoubleType(), True),
#         StructField(" Flow IAT Mean", DoubleType(), True),
#         StructField(" Flow IAT Std", DoubleType(), True),
#         StructField(" Flow IAT Max", DoubleType(), True),
#         StructField(" Flow IAT Min", DoubleType(), True),
#         StructField("Fwd IAT Total", DoubleType(), True),
#         StructField(" Fwd IAT Mean", DoubleType(), True),
#         StructField(" Fwd IAT Std", DoubleType(), True),
#         StructField(" Fwd IAT Max", DoubleType(), True),
#         StructField(" Fwd IAT Min", DoubleType(), True),
#         StructField("Bwd IAT Total", DoubleType(), True),
#         StructField(" Bwd IAT Mean", DoubleType(), True),
#         StructField(" Bwd IAT Std", DoubleType(), True),
#         StructField(" Bwd IAT Max", DoubleType(), True),
#         StructField(" Bwd IAT Min", DoubleType(), True),
#         StructField("Fwd PSH Flags", IntegerType(), True),
#         StructField(" Bwd PSH Flags", IntegerType(), True),
#         StructField(" Fwd URG Flags", IntegerType(), True),
#         StructField(" Bwd URG Flags", IntegerType(), True),
#         StructField(" Fwd Header Length", IntegerType(), True),
#         StructField(" Bwd Header Length", IntegerType(), True),
#         StructField("Fwd Packets/s", DoubleType(), True),
#         StructField(" Bwd Packets/s", DoubleType(), True),
#         StructField(" Min Packet Length", DoubleType(), True),
#         StructField(" Max Packet Length", DoubleType(), True),
#         StructField(" Packet Length Mean", DoubleType(), True),
#         StructField(" Packet Length Std", DoubleType(), True),
#         StructField(" Packet Length Variance", DoubleType(), True),
#         StructField("FIN Flag Count", IntegerType(), True),
#         StructField(" SYN Flag Count", IntegerType(), True),
#         StructField(" RST Flag Count", IntegerType(), True),
#         StructField(" PSH Flag Count", IntegerType(), True),
#         StructField(" ACK Flag Count", IntegerType(), True),
#         StructField(" URG Flag Count", IntegerType(), True),
#         StructField(" CWE Flag Count", IntegerType(), True),
#         StructField(" ECE Flag Count", IntegerType(), True),
#         StructField(" Down/Up Ratio", DoubleType(), True),
#         StructField(" Average Packet Size", DoubleType(), True),
#         StructField(" Avg Fwd Segment Size", DoubleType(), True),
#         StructField(" Avg Bwd Segment Size", DoubleType(), True),
#         StructField(" Fwd Header Length", IntegerType(), True), # Duplicate in some versions, check if needed
#         StructField("Fwd Avg Bytes/Bulk", DoubleType(), True),
#         StructField(" Fwd Avg Packets/Bulk", DoubleType(), True),
#         StructField(" Fwd Avg Bulk Rate", DoubleType(), True),
#         StructField(" Bwd Avg Bytes/Bulk", DoubleType(), True),
#         StructField(" Bwd Avg Packets/Bulk", DoubleType(), True),
#         StructField("Bwd Avg Bulk Rate", DoubleType(), True),
#         StructField("Subflow Fwd Packets", IntegerType(), True),
#         StructField(" Subflow Fwd Bytes", DoubleType(), True),
#         StructField(" Subflow Bwd Packets", IntegerType(), True),
#         StructField(" Subflow Bwd Bytes", DoubleType(), True),
#         StructField("Init_Win_bytes_forward", IntegerType(), True),
#         StructField(" Init_Win_bytes_backward", IntegerType(), True),
#         StructField(" act_data_pkt_fwd", IntegerType(), True),
#         StructField(" min_seg_size_forward", IntegerType(), True),
#         StructField("Active Mean", DoubleType(), True),
#         StructField(" Active Std", DoubleType(), True),
#         StructField(" Active Max", DoubleType(), True),
#         StructField(" Active Min", DoubleType(), True),
#         StructField("Idle Mean", DoubleType(), True),
#         StructField(" Idle Std", DoubleType(), True),
#         StructField(" Idle Max", DoubleType(), True),
#         StructField(" Idle Min", DoubleType(), True),
#         StructField(" Label", StringType(), True)
#     ])
#     return schema
import os
from pathlib import Path
import pandas as pd


DATA_ROOT = Path("data/raw")
STORE_DIR = Path("data/raw_view")


def get_the_view_of_data(path_to_dir: Path, n_rows: int = 10) -> None:
    """
    Create lightweight preview CSVs (first N rows) for each raw CSV file.
    """
    if not path_to_dir.exists():
        raise FileNotFoundError(f"Directory not found: {path_to_dir}")

    STORE_DIR.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(path_to_dir.glob("*.csv"))

    if not csv_files:
        raise RuntimeError(f"No CSV files found in {path_to_dir}")

    for csv_path in csv_files:
        try:
            df = pd.read_csv(
                csv_path,
                nrows=n_rows,
                encoding="utf-8",
                on_bad_lines="skip",
                low_memory=False,
            )

            out_name = f"view_{csv_path.stem}.csv"
            out_path = STORE_DIR / out_name
            df.to_csv(out_path, index=False)

            print(f"Saved preview: {out_path}")

        except Exception as exc:
            print(f"⚠️ Skipped {csv_path.name}: {exc}")


if __name__ == "__main__":
    get_the_view_of_data(DATA_ROOT)
