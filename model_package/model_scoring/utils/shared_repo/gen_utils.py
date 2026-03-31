# functions for using in other scripts
import datetime
import json
import logging
import os
import sys
import tempfile
import time
from functools import wraps

import boto3
import joblib
import numpy as np
import pandas as pd
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import torch
from botocore.exceptions import ClientError
from google.cloud import storage


# get secrets key
def get_creds(mycreds: str, secrets_path: str = None) -> str:
    """
    Reads a secret from a secrets.json file, either from a user-provided path or defaulting to model_scoring/secrets.json.

    Args:
        mycreds (str): Key to retrieve from the secrets file.
        secrets_path (str, optional): Optional absolute path to secrets.json. Defaults to model_scoring/secrets.json inside installed package.

    Returns:
        str: The value associated with the secret key.

    Raises:
        ValueError, FileNotFoundError, PermissionError
    """
    if secrets_path is None:
        # Default to 'secrets.json' inside the installed model_scoring package
        try:
            from model_scoring import __file__ as scoring_root
            secrets_path = Path(scoring_root).parent / "secrets.json"
        except ImportError:
            raise ImportError("model_scoring package not found; cannot resolve default secrets path.")

    secrets_path = Path(secrets_path)

    try:
        with secrets_path.open() as f:
            data = json.load(f)
            return data[mycreds]
    except FileNotFoundError:
        print(f"Error: secrets.json not found at {secrets_path}")
        sys.exit(1)
    except KeyError:
        print(f"Error: Secret key '{mycreds}' not found.")
        sys.exit(1)
    except PermissionError:
        print("Error: Permission denied while accessing secrets.json")
        sys.exit(1)


def log_time(func):
    @wraps(func)  # Preserve function name and metadata
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()  # High-precision timer
        start_timestamp = datetime.datetime.now().strftime("%H:%M:%S:%f")[:-3]  # Format start time with milliseconds

        logging.info(f"Starting {func.__name__} at {start_timestamp}")  # logs actual function name

        result = func(*args, **kwargs)

        end_time = time.perf_counter()
        execution_time = end_time - start_time

        # Convert execution time to HH:MM:SS:MS format
        ms = int((execution_time % 1) * 1000)
        seconds = int(execution_time) % 60
        minutes = (int(execution_time) // 60) % 60
        hours = int(execution_time) // 3600

        formatted_runtime = f"{hours:02}:{minutes:02}:{seconds:02}:{ms:03}"
        logging.info(f"{func.__name__} executed in {formatted_runtime}")

        return result

    return wrapper


# time elapsed record
def elapsed_time(note, project_start_time, function_start_time):
    """
    :param note: string,function name or action
    :param project_start_time: time.time() at the start of working
    :param function_start_time: time.time() at the start of running function
    :return: elapsed time of both project and function running
    """
    secs = time.time() - function_start_time
    hrs = np.int32(secs / 3600)
    secs = secs % 3600.0
    mins = np.int32(secs / 60)
    secs = secs % 60.0
    print_string = '\nElapsed time ({}): {:d}:{:d}:{:06.3f}'.format(note, hrs, mins, secs)
    secs = time.time() - project_start_time
    hrs = np.int32(secs / 3600)
    secs = secs % 3600.0
    mins = np.int32(secs / 60)
    secs = secs % 60.0
    print_string += '\nTotal elapsed time: {:d}:{:d}:{:06.3f}'.format(hrs, mins, secs)
    print(print_string)


# # for file saving and loading


def save_data(data_file, data_path, data_name, bucket_name=None, data_extension="pkl", schema=None, s3_bucket=None):
    """
    Save a Python object as a specified file format (pickle, csv, html, or parquet), either locally, to GCS, or S3.

    Args:
        data_file: The Python object to be saved. For parquet format, should be a DataFrame.
        data_path (str): The directory where the file will be saved.
        data_name (str): The name of the file (without extension).
        bucket_name (str, optional): The name of the GCS bucket. If provided, the file will be saved to GCS.
        data_extension (str, optional): The extension of the file. Can be "pkl", "csv", "html", or "parquet". Defaults to "pkl".
        schema (pyarrow.Schema, optional): The schema to be used when writing Parquet files. Required if data_extension is "parquet".
        s3_bucket (str, optional): S3 bucket name for saving.
    """
    if data_extension not in ["pkl", "csv", "html", "parquet", "pt"]:
        raise ValueError("Invalid data_extension. Must be one of: 'pkl', 'csv', 'html', 'parquet', 'pt'.")

    if s3_bucket:
        file_name = f"{data_name}.{data_extension}"
        key = os.path.join(data_path, file_name).replace("\\", "/")
        s3_key = get_creds("s3_bucket")
        s3 = boto3.resource(
            service_name='s3',
            verify=False,
            endpoint_url=s3_key['endpoint_url'],  # Replace with your endpoint
            aws_access_key_id=s3_key['aws_access_key_id'],
            aws_secret_access_key=s3_key['aws_secret_access_key'],
        )

        with tempfile.TemporaryFile() as temp_file:
            # Check if the file already exists in S3
            s3_object = s3.Object(s3_bucket, key)
            try:
                s3_object.load()  # Try to load the object to check existence
                print(f"File already exists: s3://{s3_bucket}/{key}. Deleting...")
                s3.meta.client.delete_object(Bucket=s3_bucket, Key=key)
                print(f"File deleted: s3://{s3_bucket}/{key}")
            except ClientError as e:
                if e.response['Error']['Code'] == "404":
                    pass  # File does not exist, proceed
                else:
                    logging.error(e)
                    raise RuntimeError(f"Error checking existence of file: s3://{s3_bucket}/{key}")
            try:
                # Save data to the temporary file based on extension
                if data_extension == "pkl":
                    joblib.dump(data_file, temp_file)
                elif data_extension == "csv":
                    if not isinstance(data_file, pd.DataFrame):
                        raise ValueError("For CSV format, data_file must be a DataFrame.")
                    data_file.to_csv(temp_file, index=False)
                elif data_extension == "html":
                    temp_file.write(data_file.encode("utf-8"))
                elif data_extension == "pt":
                    torch.save(data_file, temp_file)
                elif data_extension == "parquet":
                    if not isinstance(data_file, pd.DataFrame):
                        raise ValueError("For parquet format, data_file must be a DataFrame.")
                    if schema is None:
                        raise ValueError("A schema must be provided when saving data as Parquet.")
                    table = pa.Table.from_pandas(data_file, schema=schema)
                    pq.write_table(table, temp_file)

                temp_file.seek(0)  # Move to the beginning of the file
                s3.Object(s3_bucket, key).put(Body=temp_file.read())
                print(f"Data saved successfully to S3: s3://{s3_bucket}/{key}")
            except ClientError as e:
                logging.error(e)
                raise RuntimeError("Failed to save data to S3.")
            finally:
                # Close the temporary file to release resources
                temp_file.close()
                # Delete the temporary file from disk
    #                 os.remove(temp_file.name)
    elif bucket_name:
        # GCS saving logic
        blob_file_path = os.path.join(data_path, f"{data_name}.{data_extension}").replace("\\", "/")
        try:
            client = storage.Client()
            # bucket = client.get_bucket(bucket_name)
            bucket = client.bucket(bucket_name)  # Direct reference
            blob = bucket.blob(blob_file_path)

            with tempfile.NamedTemporaryFile(suffix=f".{data_extension}", delete=False) as temp_file:
                if data_extension == "pkl":
                    joblib.dump(data_file, temp_file)
                elif data_extension == "csv":
                    data_file.to_csv(temp_file.name, index=False)
                elif data_extension == "html":
                    temp_file.write(data_file.encode("utf-8"))
                elif data_extension == "pt":
                    torch.save(data_file, temp_file)
                elif data_extension == "parquet":
                    table = pa.Table.from_pandas(data_file, schema=schema)
                    pq.write_table(table, temp_file.name)

                temp_file.seek(0)
                blob.upload_from_filename(temp_file.name)
            print(f"Data saved successfully to GCS: gs://{bucket_name}/{blob_file_path}")
        except Exception as e:
            print(f"Error occurred while saving data to GCS: {e}")
        finally:
            # Close the temporary file to release resources
            temp_file.close()
            # Delete the temporary file from disk
            os.remove(temp_file.name)
    else:
        # Local saving logic
        local_file_path = os.path.join(data_path, f"{data_name}.{data_extension}")
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

        try:
            if data_extension == "pkl":
                joblib.dump(data_file, local_file_path)
            elif data_extension == "csv":
                data_file.to_csv(local_file_path, index=False)
            elif data_extension == "html":
                with open(local_file_path, "w") as f:
                    f.write(data_file)
            elif data_extension == "pt":
                    torch.save(data_file, local_file_path)
            elif data_extension == "parquet":
                table = pa.Table.from_pandas(data_file, schema=schema)
                pq.write_table(table, local_file_path)
            print(f"Data saved successfully locally as {data_extension}: {local_file_path}")
        except Exception as e:
            print(f"Error occurred while saving data locally: {e}")


def load_data(data_path, data_name, bucket_name=None, data_extension="pkl", s3_bucket=None):
    """
    Load a Python object or DataFrame from a file, either locally or from Google Cloud Storage.

    Args:
        data_path (str): The directory where the file is located.
        data_name (str): The name of the file (without extension).
        bucket_name (str, optional): The name of the GCS bucket. If provided, the file will be loaded from GCS.
        data_extension (str, optional): The extension of the file. Can be "pkl", "csv", "html", or "parquet". Defaults to "pkl".
        s3_bucket (str, optional): The name of the S3 bucket. If provided, the file will be loaded from S3.

    Returns:
        object or DataFrame: The Python object or DataFrame loaded from the file, or None if loading fails.
    """
    if data_extension not in ["pkl", "csv", "html", "parquet","pt", "pth"]:
        raise ValueError("Invalid data_extension. Must be one of: 'pkl', 'csv', 'html', 'parquet','pt', 'pth'.")
    if s3_bucket:
        file_name = f"{data_name}.{data_extension}"
        key = os.path.join(data_path, file_name).replace("\\", "/")
        s3_key = get_creds("s3_bucket")
        s3 = boto3.resource(
            service_name='s3',
            verify=False,
            endpoint_url=s3_key['endpoint_url'],  # Replace with your endpoint
            aws_access_key_id=s3_key['aws_access_key_id'],
            aws_secret_access_key=s3_key['aws_secret_access_key'],
        )

        with tempfile.TemporaryFile() as temp_file:
            try:
                # Download file from S3
                s3.meta.client.download_fileobj(Bucket=s3_bucket, Key=key, Fileobj=temp_file)
                temp_file.seek(0)  # Move to the beginning of the file
                print(f"Data loaded successfully from S3: s3://{s3_bucket}/{key}")
                # Load data based on its extension
                if data_extension == "pkl":
                    return joblib.load(temp_file)
                elif data_extension == "csv":
                    return pd.read_csv(temp_file)
                elif data_extension == "html":
                    return temp_file.read().decode("utf-8")
                elif data_extension == "parquet":
                    return pq.read_table(temp_file).to_pandas()
                elif data_extension == "pt":
                    return torch.load(temp_file, weights_only= False)
                elif data_extension == "pth":
                    return torch.load(temp_file, weights_only= False)
            except Exception as e:
                print(f"Error occurred while loading data from S3: {e}")
                return None
            finally:
                # Close the temporary file to release resources
                temp_file.close()
                # Delete the temporary file from disk
    #                 os.remove(temp_file.name)
    elif bucket_name:
        blob_file_path = os.path.join(data_path, f"{data_name}.{data_extension}")
        # Replace backslashes with forward slashes
        blob_file_path = blob_file_path.replace("\\", "/")
        try:
            # Initialize GCS client
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_file_path)
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                blob.download_to_file(temp_file)
                temp_file.seek(0)
                print(f"Data loaded successfully from GCS: gs://{bucket_name}/{blob_file_path}")
                if data_extension == "pkl":
                    return joblib.load(temp_file)
                elif data_extension == "csv":
                    return pd.read_csv(temp_file)
                elif data_extension == "html":
                    return temp_file.read().decode("utf-8")
                elif data_extension == "parquet":
                    return pq.read_table(temp_file).to_pandas()
                elif data_extension == "pt":
                    return torch.load(temp_file, weights_only= False)
                elif data_extension == "pth":
                    return torch.load(temp_file, weights_only= False)
        except Exception as e:
            print(f"Error occurred while loading data from GCS: {e}")
            return None
        finally:
            # Close the temporary file to release resources
            temp_file.close()
            # Delete the temporary file from disk
            os.remove(temp_file.name)
    else:
        file_path = os.path.join(data_path, f"{data_name}.{data_extension}")
        try:
            if os.path.exists(file_path):
                print(f"Loading data locally: {file_path}")
                if data_extension == "pkl":
                    return joblib.load(file_path)
                elif data_extension == "csv":
                    return pd.read_csv(file_path)
                elif data_extension == "html":
                    with open(file_path, "r") as f:
                        return f.read()
                elif data_extension == "parquet":
                    return pq.read_table(file_path).to_pandas()
                elif data_extension == "pt":
                    return torch.load(file_path, weights_only= False)
                elif data_extension == "pth":
                    return torch.load(file_path, weights_only= False)
            else:
                print(f"Error: File not found at {file_path}")
                return None
        except Exception as e:
            print(f"Error occurred while loading data locally: {e}")
            return None

