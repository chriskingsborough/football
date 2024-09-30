import duckdb
import boto3
import pandas as pd
from io import BytesIO

def initialize_minio(minio_config):
    """Initialize the MinIO client."""
    return boto3.client(
        's3',
        endpoint_url=f"{minio_config['endpoint']}",
        aws_access_key_id=minio_config['access_key'],
        aws_secret_access_key=minio_config['secret_key']
    )

def fetch_all_objects(s3_client, bucket_name):
    """Fetch all objects in a bucket"""

    objects = []
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        
        # Keep paginating if the response is truncated (i.e., more objects exist)
        while True:
            # Check if the bucket has contents
            if 'Contents' in response:
                for obj in response['Contents']:
                    objects.append(obj['Key'])
            
            # Check if there are more objects to list
            if response['IsTruncated']:
                continuation_token = response['NextContinuationToken']
                response = s3_client.list_objects_v2(Bucket=bucket_name, ContinuationToken=continuation_token)
            else:
                break

        return objects
    
    except Exception as e:
        raise RuntimeError(f"Failed to list objects in bucket {bucket_name}: {e}")

def fetch_csv_from_minio(s3_client, bucket_name, object_key):
    """Fetch CSV data from MinIO and return it as a Pandas DataFrame."""
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    csv_data = response['Body'].read()
    df = pd.read_csv(BytesIO(csv_data))
    return df

def load_data_to_duckdb(df, db_path, table_name):
    """Load data from DataFrame into a DuckDB table."""
    conn = duckdb.connect(db_path)
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
    conn.close()
    print(f"Data successfully loaded into {table_name} in DuckDB.")

if __name__ == "__main__":
    # Configuration for MinIO and DuckDB
    minio_config = {
        "endpoint": "http://localhost:9000",
        "access_key": "user",
        "secret_key": "password"
    }
    db_path = "data/warehouse/football.db"
    bucket_name = "football-data"
    # object_key = "your-object.csv"
    # table_name = "bronze_table"

    # Run the functions
    s3_client = initialize_minio(minio_config)
    objects = fetch_all_objects(s3_client, bucket_name)
    for object_key in objects:
        df = fetch_csv_from_minio(s3_client, bucket_name, object_key)
        table_name = object_key.split('/')[-1].replace('.csv','') # take the end
        load_data_to_duckdb(df, db_path, table_name)

