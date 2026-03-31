import os
import tempfile
import joblib
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import torch

# for file saving and loading
def save_data(
    data_file, data_path, data_name, bucket_name=None, data_extension="pkl", schema=None
):
    """
    Save a Python object as a specified file format (pickle, csv, html, or parquet), either locally or to Google Cloud Storage.

    Args:
        data_file: The Python object to be saved. For parquet format, should be a DataFrame.
        data_path (str): The directory where the file will be saved.
        data_name (str): The name of the file (without extension).
        bucket_name (str, optional): The name of the GCS bucket. If provided, the file will be saved to GCS.
        data_extension (str, optional): The extension of the file. Can be "pkl", "csv", "html", or "parquet". Defaults to "pkl".
        schema (pyarrow.Schema, optional): The schema to be used when writing Parquet files.
                                           Required if data_extension is "parquet".
    """
    if data_extension not in ["pkl", "csv", "html", "parquet", "pt"]:
        raise ValueError(
            "Invalid data_extension. Must be one of: 'pkl', 'csv', 'html', 'parquet', 'pt'."
        )

    if bucket_name is None:
        local_file_path = os.path.join(data_path, f"{data_name}.{data_extension}")
        try:
            os.makedirs(
                os.path.dirname(local_file_path), exist_ok=True
            )  # Ensure directory exists
            if data_extension == "pkl":
                joblib.dump(data_file, local_file_path)
                print(f"Data saved successfully locally as pickle: {local_file_path}")
            elif data_extension == "csv":
                data_file.to_csv(local_file_path, index=False)
                print(f"Data saved successfully locally as CSV: {local_file_path}")
            elif data_extension == "html":
                with open(local_file_path, "w") as f:
                    f.write(data_file)
                print(f"Data saved successfully locally as HTML: {local_file_path}")
            elif data_extension == "parquet":
                if not isinstance(data_file, pd.DataFrame):
                    raise ValueError(
                        "For parquet format, data_file must be a DataFrame."
                    )
                if schema is None:
                    raise ValueError(
                        "A schema must be provided when saving data as Parquet."
                    )
                table = pa.Table.from_pandas(data_file, schema=schema)
                pq.write_table(table, local_file_path)
                print(f"Data saved successfully locally as Parquet: {local_file_path}")
            elif data_extension == "pt":
                torch.save(data_file, local_file_path)
                print(f"Data saved successfully locally as PyTorch: {local_file_path}")
        except Exception as e:
            print(f"Error occurred while saving data locally: {e}")
    else:
        blob_file_path = os.path.join(data_path, f"{data_name}.{data_extension}")
        # Replace backslashes with forward slashes
        blob_file_path = blob_file_path.replace("\\", "/")
        try:
            # Initialize GCS client
            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(blob_file_path)
            if data_extension == "pkl":
                with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                    joblib.dump(data_file, temp_file)
                    temp_file.seek(0)
                    blob.upload_from_file(temp_file)
                print(
                    f"Data saved successfully to GCS as pickle: gs://{bucket_name}/{data_path}/{data_name}.{data_extension}"
                )
            elif data_extension == "csv":
                with tempfile.NamedTemporaryFile(
                    suffix=".csv", delete=False
                ) as temp_file:
                    data_file.to_csv(temp_file.name, index=False)
                    temp_file.seek(0)
                    blob.upload_from_file(temp_file)
                print(
                    f"Data saved successfully to GCS as CSV: gs://{bucket_name}/{data_path}/{data_name}.{data_extension}"
                )
            elif data_extension == "html":
                with tempfile.NamedTemporaryFile(
                    suffix=".html", delete=False
                ) as temp_file:
                    temp_file.write(data_file.encode("utf-8"))
                    temp_file.seek(0)
                    blob.upload_from_file(temp_file)
                print(
                    f"Data saved successfully to GCS as HTML: gs://{bucket_name}/{data_path}/{data_name}.{data_extension}"
                )
            elif data_extension == "parquet":
                if not isinstance(data_file, pd.DataFrame):
                    raise ValueError(
                        "For parquet format, data_file must be a DataFrame."
                    )
                if schema is None:
                    raise ValueError(
                        "A schema must be provided when saving data as Parquet."
                    )
                with tempfile.NamedTemporaryFile(
                    suffix=".parquet", delete=False
                ) as temp_file:
                    table = pa.Table.from_pandas(data_file, schema=schema)
                    pq.write_table(table, temp_file.name)
                    temp_file.seek(0)
                    blob.upload_from_file(temp_file)
                print(
                    f"Data saved successfully to GCS as Parquet: gs://{bucket_name}/{data_path}/{data_name}.{data_extension}"
                )
        except Exception as e:
            print(f"Error occurred while saving data to GCS: {e}")
        finally:
            # Close the temporary file to release resources
            temp_file.close()
            # Delete the temporary file from disk
            os.remove(temp_file.name)


def load_data(data_path, data_name, bucket_name=None, data_extension="pkl"):
    """
    Load a Python object or DataFrame from a file, either locally or from Google Cloud Storage.

    Args:
        data_path (str): The directory where the file is located.
        data_name (str): The name of the file (without extension).
        bucket_name (str, optional): The name of the GCS bucket. If provided, the file will be loaded from GCS.
        data_extension (str, optional): The extension of the file. Can be "pkl", "csv", "html", or "parquet". Defaults to "pkl".

    Returns:
        object or DataFrame: The Python object or DataFrame loaded from the file, or None if loading fails.
    """
    if data_extension not in ["pkl", "csv", "html", "parquet", "pt", "pth"]:
        raise ValueError(
            "Invalid data_extension. Must be one of: 'pkl', 'csv', 'html', 'parquet','pt', 'pth'."
        )

    if bucket_name is None:
        file_path = os.path.join(data_path, f"{data_name}.{data_extension}")
        try:
            if os.path.exists(file_path):
                print(f"Loading data locally: {file_path}")
                if data_extension == "pkl":
                    return joblib.load(file_path)
                elif data_extension == "csv":
                    return pd.read_csv(file_path, low_memory=False)
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
    else:
        blob_file_path = os.path.join(data_path, f"{data_name}.{data_extension}")
        # Replace backslashes with forward slashes
        blob_file_path = blob_file_path.replace("\\", "/")
        try:
            # Initialize GCS client
            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(blob_file_path)
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                blob.download_to_file(temp_file)
                temp_file.seek(0)
                if data_extension == "pkl":
                    return joblib.load(temp_file)
                elif data_extension == "csv":
                    return pd.read_csv(temp_file)
                elif data_extension == "html":
                    return temp_file.read().decode("utf-8")
                elif data_extension == "parquet":
                    return pq.read_table(temp_file).to_pandas()
        except Exception as e:
            print(f"Error occurred while loading data from GCS: {e}")
            return None
        finally:
            # Close the temporary file to release resources
            temp_file.close()
            # Delete the temporary file from disk
            os.remove(temp_file.name)
