
import os
import pandas as pd
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession

# Enable Hive support and specify warehouse directory
spark = SparkSession.builder.getOrCreate()

def populate_file_dict_list(directory: str) -> list:
    
    file_dict_list = []
    file_delimiter = ''
    
    for file_name in os.listdir(directory):
        if file_name.endswith(".txt"):
            file_delimiter = '\t'
        else:
            file_delimiter = ','    

        # Check if the file is a directory
        if not os.path.isdir(os.path.join(directory, file_name)):
            
            file_info = {'file_to_load': file_name,'file_delimiter':file_delimiter}
            file_dict_list.append(file_info)

    return file_dict_list

def create_artifact_csv(df: pd.DataFrame, csv_file_name: str) -> str:
    
    # 1. Get the output directory from the environment variable
    artifact_output_dir = os.environ.get("BUILD_ARTIFACTSTAGINGDIRECTORY")

    # 2. Check if the environment variable is set (CRUCIAL)
    if artifact_output_dir is None:
        raise ValueError("BUILD_ARTIFACTSTAGINGDIRECTORY environment variable is not set.")

    # 3. Construct the full CSV file path
    csv_path = os.path.join(artifact_output_dir, csv_file_name)

    # 4. (Optional but Recommended) Create the directory if it doesn't exist 
    os.makedirs(artifact_output_dir, exist_ok=True)

    # 5. Save the DataFrame to CSV
    df.toPandas().to_csv(csv_path, header=False, index=False)

    # 6. Print the CSV file path
    print(f"CSV file saved to: {csv_path}")

    # 7. Return the CSV file path
    return csv_path

def convert_to_parquet(file_info: dict) -> None:
    
    file_name_date = file_info['file_to_load'].split('.')[0]
    file_date = file_name_date.split('_')[1]
    file_name = file_name_date.split('_')[0].lower()
    year = file_date[:4]
    month = file_date[4:6]
    day = file_date[6:8]
    
    if not os.path.exists(f'data_lake/{file_name}/year={year}/month={month}/day={day}') == True:
    
        file_path = f'data/{file_info["file_to_load"]}'
        
        df = spark.read.format("csv").load(file_path, header=False, inferSchema=True, delimiter=file_info['file_delimiter'])
        
        df = df.withColumn('CreatedDate', lit(f"{year}-{month}-{day}")).withColumn('UpdatedDate', lit(f"{year}-{month}-{day}"))

        output_dir = f"data_lake/{file_name}/year={year}/month={month}/day={day}"
        os.makedirs(output_dir, exist_ok=True)
        
        df.write.parquet(output_dir, mode='overwrite')
        
        print(f'{file_name_date} moved to parquet')
        
        # Create database if not exists
        spark.sql("CREATE DATABASE IF NOT EXISTS default")
        df.write.mode("overwrite").saveAsTable("default.my_table") 
        
        # Show tables
        spark.sql("SELECT * FROM default.my_table LIMIT 10").show()
        
        create_artifact_csv(df,file_info["file_to_load"])
        
    else:
        print(f'{file_name_date} already exists in parquet')

def load_data_lake():

    # checks if a directory specified by `data_lake_path` exists. 
    data_lake_path = f'data_lake/'

    if not os.path.exists(data_lake_path):
        os.makedirs(data_lake_path)
        print(f'{data_lake_path} created')
    else:
        print(f'{data_lake_path} already exists')

    # Example usage
    directory = f'data'
    file_dict_list = populate_file_dict_list(directory)
    print(file_dict_list)

    for file_info in file_dict_list:
        convert_to_parquet(file_info)
    
# docker-compose up -d

# docker build -t spark-job .
# docker run --network=host spark-job

# docker exec -it spark-master spark-sql
# SELECT * FROM default.my_table LIMIT 10;