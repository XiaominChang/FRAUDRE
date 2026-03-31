# functions for using in other scripts
import os
import tempfile
import time

import joblib
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
# from google.cloud import storage


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
    print_string = "\nElapsed time ({}): {:d}:{:d}:{:06.3f}".format(
        note, hrs, mins, secs
    )
    secs = time.time() - project_start_time
    hrs = np.int32(secs / 3600)
    secs = secs % 3600.0
    mins = np.int32(secs / 60)
    secs = secs % 60.0
    print_string += "\nTotal elapsed time: {:d}:{:d}:{:06.3f}".format(hrs, mins, secs)
    print(print_string)


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
    if data_extension not in ["pkl", "csv", "html", "parquet"]:
        raise ValueError(
            "Invalid data_extension. Must be one of: 'pkl', 'csv', 'html', 'parquet'."
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
    if data_extension not in ["pkl", "csv", "html", "parquet"]:
        raise ValueError(
            "Invalid data_extension. Must be one of: 'pkl', 'csv', 'html', 'parquet'."
        )

    if bucket_name is None:
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


def upload_to_gcs(local_file_path, bucket_name, destination_blob_name, download=False):
    """
    Uploads or downloads a file to/from a Google Cloud Storage bucket.

    Parameters:
    - local_file_path: Local file path to upload or download.
    - bucket_name: Name of the GCS bucket.
    - destination_blob_name: Destination blob name in the GCS bucket.
    - download: If True, download the file from GCS. If False, upload the file to GCS. Default is False.
    """
    try:
        # Initialize GCS client
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        local_file_path = local_file_path.replace("\\", "/")
        destination_blob_name = destination_blob_name.replace("\\", "/")
        if download:
            # Download the file from GCS
            blob = bucket.blob(destination_blob_name)
            blob.download_to_filename(local_file_path)
            print(
                f"File downloaded successfully from GCS: gs://{bucket_name}/{destination_blob_name}"
            )
        else:
            # Upload the file to GCS
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(local_file_path)
            print(
                f"File uploaded successfully to GCS: gs://{bucket_name}/{destination_blob_name}"
            )

    except Exception as e:
        print(
            f"Error occurred while {'downloading' if download else 'uploading'} file to GCS: {e}"
        )


def delete_blob(bucket_name, blob_name, del_folder=False):
    """Deletes a blob or a folder from the bucket."""
    try:
        # Initialize GCS client
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)

        if del_folder:
            # List all blobs in the folder
            blobs = bucket.list_blobs(prefix=blob_name)

            # Delete each blob
            for blob in blobs:
                blob.delete()
                print(f"Blob deleted successfully: gs://{bucket_name}/{blob.name}")

            print(f"Folder deleted successfully: gs://{bucket_name}/{blob_name}")
        else:
            # Specify the blob to be deleted
            blob = bucket.blob(blob_name)

            # Delete the blob
            blob.delete()
            print(f"Blob deleted successfully: gs://{bucket_name}/{blob_name}")

    except Exception as e:
        print(f"Error occurred while deleting blob: {e}")


def bucket_size(bucket_name):
    """
    Calculate the total size of all objects in a Google Cloud Storage bucket.

    Args:
        bucket_name (str): The name of the GCS bucket.

    Returns:
        float: The total size of all objects in the bucket in gigabytes (GB).
    """
    size_byte = 0
    # Initialize GCS client
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for blob in bucket.list_blobs():
        size_byte += blob.size

    # Convert bytes to gigabytes
    total_size_gb = size_byte / (1024 * 1024 * 1024)

    return total_size_gb


# List objects in a bucket or sub folder
def gs_objs(bucket_name, prefix=None):
    """
    List objects in a Google Cloud Storage bucket.

    Parameters:
    - bucket_name: Name of the GCS bucket.
    - prefix: (Optional) Prefix to filter objects by a common path prefix.

    Returns:
    - List of object names in the bucket or under the specified prefix.
    """
    # Initialize GCS client
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    # List blobs with optional prefix
    blobs = bucket.list_blobs(prefix=prefix)

    # Extract folder and object names
    folders = set()
    objects = []
    for blob in blobs:
        if blob.name.endswith("/"):  # Check if it's a folder
            folders.add(blob.name)
        else:
            # Extract folder path and add to the set
            folder_path = "/".join(blob.name.split("/")[:-1]) + "/"
            folders.add(folder_path)

        objects.append(blob.name)

    # Sort folders and objects
    folders_sorted = sorted(list(folders))
    objects_sorted = sorted(objects)

    # Combine folders and objects
    object_names = folders_sorted + objects_sorted

    return object_names


def data_summary(data, save_loc):
    dff = data
    df_exp = pd.DataFrame(
        {
            "name": dff.columns,
            "non-nulls": len(dff) - dff.isnull().sum().values,
            "nulls": dff.isnull().sum().values,
            "type": dff.dtypes.values,
        }
    )
    if save_loc is not None:
        df_exp.to_csv(save_loc, index=False)

