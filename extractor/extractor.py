import os
import pandas as pd

class Extractor:
    def __init__(self, input_path: str, bronze_path: str):
        self.input_path = input_path
        self.bronze_path = bronze_path

    def create_folder(self, path: str):
        os.makedirs(path, exist_ok=True)

    def get_mitma_partition_path(self, filename: str) -> str:
        """
        Generate the path of the partition based on the filename.
        
        Example:
            filename = "20220101_Pernoctaciones_distritos.csv.gz"
            -> bronze/mitma/2022/01/01
        """
        date_str = filename[:8]
    
        year = date_str[:4]
        month = date_str[4:6]
        day = date_str[6:8]

        partition_path = os.path.join(
            "mitma",
            f"{year}",
            f"{month}",
            f"{day}"
        )
        return partition_path
    
    def extract_generic(self, source_folder: str, dataset_name: str, target_folder: str, source: str):
        input_path = os.path.join(self.input_path, source_folder)

        input_file = os.path.join(input_path, f"{dataset_name}.csv")

        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Could not find {source} file: {input_file}")
        
        
        df = pd.read_csv(input_file, sep=";", encoding="latin-1", low_memory=False)

        output_path = os.path.join(self.bronze_path, target_folder)

        self.create_folder(output_path)

        output_file = os.path.join(output_path, f"{dataset_name}.parquet")
        df.to_parquet(output_file, index=False)

        print(f"[BRONZE][{source}] Stored: {output_file}")

    def extract_ine(self, dataset_name: str):
        return self.extract_generic("ine", dataset_name, "ine", "INE")

    def extract_mitma(self, dataset_name: str):
        self.extract_generic("mitma/zonificacion", dataset_name, "mitma", "MITMA")

    def extract_mitma_temporal(self, entity: str, type: str):
        specific_path ="mitma/records"
        base_path = self.input_path + "/" + specific_path

        subfolder = os.path.join(type, entity)

        input_folder = os.path.join(base_path, subfolder)
        if not os.path.isdir(input_folder):
            raise FileNotFoundError(f"Folder does not exist: {input_folder}")
        
        files = [
            f for f in os.listdir(input_folder)
            if f.endswith(".csv.gz") or f.endswith(".csv")
        ]

        if not files:
            raise FileNotFoundError(f"Files not found in: {input_folder}")
        
        print(f"Processing {len(files)} files from {input_folder} ...")

        for file in files:
            input_file = os.path.join(input_folder, file)

            if not os.path.exists(input_file):
                raise FileNotFoundError(f"Could not find MITMA file: {input_file}")
            
            df = pd.read_csv(input_file, sep=";", compression="gzip", encoding="latin-1")

            partition_base = self.get_mitma_partition_path(file)

            output_path = os.path.join(self.bronze_path, partition_base, entity)
            self.create_folder(output_path)

            output_file = os.path.join(output_path, f"{type}.parquet")
            df.to_parquet(output_file, index=False)

            print(f"[BRONZE][MITMA] Stored: {output_file}")