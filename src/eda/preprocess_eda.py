# load the libraries and initial settings
import os
import sys
import tempfile
import time

import numpy as np
import pandas as pd
import pathlib
import pyarrow as pa

# add working dir to the path
cur_path = pathlib.Path(__file__).resolve().parent.parent.parent.absolute()
src_loc = cur_path.joinpath("src")
util_loc = src_loc.joinpath("utils")
sys.path.append(str(cur_path))
sys.path.append(str(src_loc))
sys.path.append(str(util_loc))

import __main__
from google.cloud import storage
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.pipeline import Pipeline
from ydata_profiling import ProfileReport

from src.conf import Conf
from src.utils.utils import elapsed_time, load_data, save_data
from utils.feature_processing import CombinedAttributesAdder

__main__.CombinedAttributesAdder = CombinedAttributesAdder


def load_locfile(conf):
    """Load data locally."""
    print("*" * 60)
    print("Loading data locally ...")

    df_orig = load_data(conf.data_gs, "modelling_data", conf.bucket_name, "parquet")
    print("*" * 60)
    print("Loading successfully")
    return df_orig


def preprocess_data(df_orig, conf, text_embedding=True, generate_report=True):
    """Preprocess data."""
    print("*" * 60)
    print("Drop duplicates from original data")
    df_proc = df_orig.drop_duplicates(subset=["claim_number"], keep="first")

    print(
        "Original sample size is {}\nSample size after De-duplicates is {}".format(
            df_orig.shape[0], df_proc.shape[0]
        )
    )
    print("*" * 60)
    print("Data ready for preprocessing")

    print("*" * 60)
    print("Preprocessing data ...")
    # condition for modelling data
    condition_date = df_proc["claim_lodgement_date"].between(
        conf.condition_start, conf.condition_end
    )
    condition_policy = df_proc["policy_details"].isin(conf.condition_policy)
    condition_rego = ~df_proc["insured_rego"].isnull()
    df_proc = df_proc.loc[condition_date & condition_policy & condition_rego]

    df_pipeline = Pipeline([("attr_adder", CombinedAttributesAdder())])
    df_totl = df_pipeline.fit_transform(df_proc)
    print("*" * 60)
    print("Preprocessing finished")
    if text_embedding:
        # tuning cleaned claim description
        x_hp = df_totl["claim_description_cleaned"]
        y_hp = df_totl["glass_rp_flag"]
        X_train_hp, X_test_hp, y_train_hp, y_test_hp = train_test_split(
            x_hp, y_hp, test_size=0.2, random_state=815
        )
        print("Start tuning tfidf......")
        function_start_time = time.time()

        pipe = Pipeline(
            [
                ("tfidf", TfidfVectorizer(stop_words="english")),
                ("clf", LogisticRegression(multi_class="ovr", solver="liblinear")),
            ]
        )

        param_grid = {
            "tfidf__max_features": [1000, 1500, 2000],
            "tfidf__max_df": (0.25, 0.3, 0.35),
            "tfidf__min_df": (0.0001, 0.001, 0.005),
            "tfidf__ngram_range": [(1, 1), (1, 2), (1, 3)],
        }
        grid = GridSearchCV(pipe, param_grid, cv=5, scoring="neg_log_loss")

        model = grid.fit(X_train_hp, y_train_hp)
        print("*" * 60)
        print("tfidf HP tuning finished")
        best_param = model.best_params_
        my_param = {}
        for key, value in best_param.items():
            new_key = key.replace("tfidf__", "")
            my_param[new_key] = value
    else:
        my_param = {
            "max_df": 0.35,
            "max_features": 2000,
            "min_df": 0.0001,
            "ngram_range": (1, 2),
        }
    # apply tfidf transformation and concatenate to original df
    tfidf_vectorizer = TfidfVectorizer(**my_param)
    data_words = df_totl["claim_description_cleaned"]

    print("Start embedding text......")
    function_start_time = time.time()
    # Transform text data using TfidfVectorizer
    tfidf_matrix = tfidf_vectorizer.fit_transform(data_words)
    tf_df = (
        pd.DataFrame.sparse.from_spmatrix(
            tfidf_matrix,
            columns=tfidf_vectorizer.get_feature_names_out(),
        )
        .sparse.to_dense()
        .fillna(0)
    )

    # Concatenate transformed features with the original dataframe
    df_processed = pd.concat(
        [df_totl.reset_index(drop=True), tf_df.reset_index(drop=True)], axis=1
    )

    # Print processing completion message
    print("Text embedding finished")

    print("*" * 60)
    print("Checking null and inf values ...")

    null_counts = df_processed.isnull().sum()
    total_rows = len(df_processed)
    null_percentages = (null_counts / total_rows) * 100
    null_info = pd.DataFrame(
        {"Null Count": null_counts, "Null Percentage": null_percentages}
    )
    null_info = null_info[null_info["Null Count"] > 0]
    print("Columns with null values and their percentage of null:")
    print(null_info)

    columns_with_inf = df_processed.columns[
        (df_processed == np.inf).any() | (df_processed == -np.inf).any()
    ].tolist()
    inf_counts = df_processed[columns_with_inf].apply(lambda col: np.isinf(col).sum())
    inf_percentages = (inf_counts / total_rows) * 100
    print("Columns with infinite values and their percentage of infinite values:")
    for column in columns_with_inf:
        print(f"Column: {column}, Infinite Percentage: {inf_percentages[column]}%")

    print("*" * 60)
    print("generate pa schema and save data")
    # Convert features to desired data types
    convert_dict = {
        "loss_hour": "int64",
        "loss_wday": "int64",
    }

    # Add conditions to check if features are in training_features
    for feature, dtype in convert_dict.items():
        if feature in df_processed.columns:
            df_processed[feature] = df_processed[feature].astype(dtype)

    # set mappings
    numpy_to_arrow_dtype = {
        "int64": pa.int64(),
        "int32": pa.int64(),
        "float64": pa.float64(),
        "float32": pa.float64(),
        "string": pa.string(),
        "object": pa.string(),
        "category": pa.dictionary(pa.int64(), pa.string()),
        "datetime64[ns]": pa.timestamp("ns"),
        "datetime64[ns, UTC]": pa.timestamp("ns"),
        "timedelta64[ns]": pa.duration("ns"),
        "bool": pa.bool_(),
    }

    # Generate PyArrow schema from DataFrame data types
    arrow_fields = []
    for col, dtype in df_processed.dtypes.items():
        if dtype.name in numpy_to_arrow_dtype:
            arrow_fields.append(pa.field(col, numpy_to_arrow_dtype[dtype.name]))
        else:
            print(
                f"Warning: Unsupported data type '{dtype.name}' for column '{col}'. Skipping."
            )

    # Create PyArrow schema
    traindf_schema = pa.schema(arrow_fields)
    # save artefacts
    # preprocessing pipeline
    save_data(df_pipeline, conf.data_gs, "processed_pipe_final", conf.bucket_name)
    # tfvct transform pipeline
    save_data(tfidf_vectorizer, conf.data_gs, "tfvct_tran_final", conf.bucket_name)
    # pa schema
    save_data(traindf_schema, conf.data_gs, "traindf_schema", conf.bucket_name)
    # processed data
    save_data(
        df_processed,
        conf.data_gs,
        "tfvct_data_final",
        conf.bucket_name,
        "parquet",
        traindf_schema,
    )

    print("*" * 60)
    print("Data preprocessing completed")
    if generate_report:
        print("*" * 60)
        print("Generating profile report ...")
        use_cols = (
            conf.num_features
            + conf.ord_features
            + conf.ohe_features
            + conf.target_feature
        )
        data_summary = ProfileReport(
            df_processed.loc[:, use_cols], missing_diagrams={"heatmap": False}
        )
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as temp_file:
            # Save the profile report to the temporary file
            data_summary.to_file(temp_file.name)
            temp_file_path = temp_file.name
        # Define the destination file path
        if conf.bucket_name is None:
            # Save locally if bucket name is not provided
            destination_path = os.path.join(conf.data_gs, "eda_report.html")
            destination_path = destination_path.replace("\\", "/")
            # Remove the destination file if it already exists
            if os.path.exists(destination_path):
                os.remove(destination_path)
            # Copy the file to the destination path
            os.rename(temp_file_path, destination_path)
            print(f"Profile report saved to local file: {destination_path}")
        else:
            # Upload to GCS bucket if bucket name is provided
            # Initialize GCS client
            client = storage.Client()
            bucket = client.get_bucket(conf.bucket_name)
            # Define the blob path in the GCS bucket
            blob_path = os.path.join(conf.data_gs, "eda_report.html")
            blob_path = blob_path.replace("\\", "/")
            # Upload the file to the GCS bucket
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(temp_file_path)
            print(
                f"Profile report uploaded to GCS bucket: gs://{conf.bucket_name}/{blob_path}"
            )
        # Delete the temporary file from disk
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        print("*" * 60)
        print("Profile report generated successfully")
    return df_processed


def main_process(text_embedding=True, generate_report=True, s_number=None):
    # Load configuration
    confparam_path = cur_path.joinpath("src", "conf", "conf_dev.yml")
    dataparam_path = cur_path.joinpath("src", "data", "dbt_model", "dbt_project.yml")
    conf = Conf(confparam_path, dataparam_path, s_number)
    project_start_time = time.time()
    df_orig = load_locfile(conf)
    function_start_time = time.time()
    df_processed = preprocess_data(
        df_orig, conf, text_embedding=text_embedding, generate_report=generate_report
    )
    elapsed_time("data processing", project_start_time, function_start_time)


if __name__ == "__main__":
    s_number = input(
        "Your s_number (e.g., sxxxxxx, otherwise s_number will be None): "
    ).lower()
    # Check if s_number starts with 's'
    if not s_number.startswith("s"):
        s_number = None
    text_embedding = (
        input(
            "Run tfidf HP tuning (e.g.,Ture or False, if not tuning, will use existing params): "
        ).lower()
        == "true"
    )
    main_process(text_embedding=text_embedding, generate_report=True, s_number=s_number)
