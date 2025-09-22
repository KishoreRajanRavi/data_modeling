# Databricks notebook source

import os

def load_csvs_to_delta(csv_dir: str, catalog_name: str, schema_name: str):
    """
    Load all CSV files from a given directory into Delta tables.
    
    Parameters:
        csv_dir (str): Path to directory containing CSV files.
        catalog_name (str): Target catalog name.
        schema_name (str): Target schema name.
    """
    
 
    
    # List CSV files in the directory
    csv_files = [f for f in dbutils.fs.ls(csv_dir) if f.name.endswith(".csv")]
    
    if not csv_files:
        raise ValueError(f"No CSV files found in {csv_dir}")
    
    for file in csv_files:
        file_path = os.path.join(csv_dir, file.name)
        table_name = os.path.splitext(file.name)[0]  # filename without .csv extension
        
        print(f"Loading {file_path} into {catalog_name}.{schema_name}.{table_name}")
        
        # Read CSV
        df = spark.read.option("header", "true").csv(file_path)
        
        # Write as Delta table
        df.write.format("delta").mode("overwrite").saveAsTable(
            f"{catalog_name}.{schema_name}.{table_name}"
        )


# COMMAND ----------

csv_dir = "/Volumes/datamodeling/data_modeling/raw_data/csv_files/"
catalog_name = "datamodeling"
schema_name = "data_modeling"

load_csvs_to_delta(csv_dir, catalog_name, schema_name)

# COMMAND ----------

