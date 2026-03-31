# %% load the libraries and initial settings
import datetime
import json
import os
import subprocess
import sys
import time

import pandas as pd
import pathlib
import pyarrow as pa
import yaml

# %% add working dir to the path
cur_path = pathlib.Path(__file__).resolve().parent.parent.parent.absolute()
src_loc = cur_path.joinpath("src")
sys.path.append(str(cur_path))
sys.path.append(str(src_loc))
from src.conf import Conf
from src.utils.sql import sql
from src.utils.utils import data_summary, elapsed_time, load_data, save_data

# %% Functions

def run_data_model(conf, run_dbt=True, final_model=True):
    """Run the DBT data model."""
    if run_dbt:

        data = json.loads(conf.secret_data)
        # Define the path to the YAML file
        yml_path = os.path.join(src_loc, "data", ".dbt", "profiles.yml")
        # Check if the YAML file already exists
        if not os.path.exists(yml_path):
            # Define the YAML content
            yaml_content = {
                "config": {"send_anonymous_usage_stats": False},
                "dbt_model": {
                    "outputs": {
                        "prod": {
                            "type": "{{ env_var(''POSTGRES_TYPE'') }}",
                            "host": "{{ env_var('POSTGRES_HOST') }}",
                            "user": "{{ env_var('POSTGRES_USERNAME') }}",
                            "password": "{{ env_var('POSTGRES_PASSWORD') }}",
                            "port": "{{ env_var('POSTGRES_PORT') | as_number }}",
                            "dbname": "{{ env_var('POSTGRES_DATABASE') }}",
                            "schema": "{{ env_var('POSTGRES_SCHEMA') }}",
                            "connect_timeout": 20,  # default 10 seconds
                            "retries": 3,  # default 1 retry on error/timeout when opening connections
                            "threads": 3,
                        }
                    },
                    "target": "prod",
                },
            }

            # Save the YAML content to the file
            with open(yml_path, "w") as file:
                yaml.dump(yaml_content, file)
        else:
            print("YAML file already exists. Skipping generation.")
        # Run DBT commands
        try:
            # set env value
            os.environ["POSTGRES_TYPE"] = conf.type
            os.environ["POSTGRES_HOST"] = conf.host
            os.environ["POSTGRES_USERNAME"] = data["EDH_PROD"]["username"]
            os.environ["POSTGRES_PASSWORD"] = data["EDH_PROD"]["password"]
            os.environ["POSTGRES_PORT"] = str(conf.port)
            os.environ["POSTGRES_DATABASE"] = conf.database
            os.environ["POSTGRES_SCHEMA"] = conf.schema
            # set profiles path
            os.environ["DBT_PROFILES_DIR"] = os.path.join(src_loc, "data", ".dbt")
            # set project path
            project_dir = os.path.join(src_loc, "data", "dbt_model")
            # Clean the DBT project
            result_cls = subprocess.run(
                ["dbt", "clean", "--project-dir", project_dir],
                capture_output=True,
                text=True,
            )
            print(result_cls.stdout)

            # Debug the DBT project
            result_dbg = subprocess.run(
                ["dbt", "debug", "--project-dir", project_dir],
                capture_output=True,
                text=True,
            )
            print(result_dbg.stdout)

            if final_model:
                # Run the specified model (choose which dataset to refresh - All or Final)
                # Define the existing DBT command arguments
                dbt_args = ["dbt", "run", "--select", "+mad_modelling_data_final_dbt"]

                # Add the --project-dir argument to the command
                dbt_args.extend(["--project-dir", project_dir])

                # Run the command with subprocess
                result_run = subprocess.run(dbt_args, capture_output=True, text=True)
                print(result_run.stdout)
            else:
                # Alternatively, run all models using:
                result_run = subprocess.run(
                    ["dbt", "run", "--project-dir", project_dir],
                    capture_output=True,
                    text=True,
                )
                print(result_run.stdout)

            # Generate documentation
            # Define the existing DBT command arguments
            dbt_args = ["dbt", "docs", "generate"]

            # Add the --project-dir argument to the command
            dbt_args.extend(["--project-dir", project_dir])

            # Run the command with subprocess
            subprocess.run(dbt_args)

            # Serve documentation (optional)
            # subprocess.run(["dbt", "docs", "serve"])

            print("DBT model execution completed successfully.")
        except subprocess.CalledProcessError as e:
            print("Error occurred while running DBT model:", e)


def query_and_save_data(conf):
    """Query data and save locally."""
    print("*" * 60)
    print("Data query started ...")

    create_date = datetime.datetime.now().strftime("%Y%m%d")
    query = """SELECT
    -- Generate random claim_number as a random string
    substring(md5(random()::text) from 1 for 6) AS claim_number,

    -- Generate random claim_lodgement_date within the last year
    (CURRENT_DATE - (random() * 365)::int) AS claim_lodgement_date,

    -- Generate random claim_loss_date within the last year
    (CURRENT_DATE - (random() * 365)::int) AS claim_loss_date,

    -- Generate random vehicle_rego_number as a random string
    substring(md5(random()::text) from 1 for 6) AS insured_rego,

    -- Generate random claim_description as a random string
    CASE
        WHEN random() < 0.3333 THEN 'driving along and kangaroo impacted'
        WHEN random() < 0.6666 THEN 'My driver was parked near a traffic light'
        ELSE 'was driving when a rock has hit the windows'
    END AS claim_description,

    -- Generate random cause_of_loss_name from predefined list
    CASE
        WHEN random() < 0.3333 THEN 'Damaged Whilst Parked'
        WHEN random() < 0.6666 THEN 'Malicious Damage'
        ELSE 'Damaged Whilst Driving'
    END AS cause_of_loss_name,

    -- Generate random general_nature_of_loss_name from predefined list
    CASE
        WHEN random() < 0.3333 THEN 'Glass (Windscreen)'
        WHEN random() < 0.6666 THEN 'Damage'
        ELSE 'Collision'
    END AS general_nature_of_loss_name,

    -- Generate random policy_details as a random string
    CASE
        WHEN random() < 0.3333 THEN 'NRMA'
        WHEN random() < 0.6666 THEN 'SGIO'
        ELSE 'SGIC'
    END AS policy_details,

    -- Generate random sum_insured between 0 and 10000
    CAST(FLOOR(random() * 10001) AS INT) AS sum_insured,

    -- Select random sum_insured_type_name from ['A', 'M']
    CASE
        WHEN random() < 0.5 THEN 'A'
        ELSE 'M'
    END AS sum_insured_type_name,

    -- Create a blank column for other_regos
    '' AS other_regos,

    -- Select random party_name from ['Instant', 'Novus', 'Obrien']
    CASE
        WHEN random() < 0.3333 THEN 'Instant'
        WHEN random() < 0.6666 THEN 'Novus'
        ELSE 'Obrien'
    END AS party_name,

    -- Select random glass_job_type from ['front', 'rear', 'frontside']
    CASE
        WHEN random() < 0.3333 THEN 'front'
        WHEN random() < 0.6666 THEN 'rear'
        ELSE 'frontside'
    END AS glass_job_type,

    -- Select random glass_rp_flag from ['others', 'chip opportunity', 'chip repair']
    CASE
        WHEN random() < 0.3333 THEN 'others'
        WHEN random() < 0.6666 THEN 'chip opportunity'
        ELSE 'chip repair'
    END AS glass_rp_flag

    FROM
        generate_series(1, 10000) AS s"""
    df_orig = sql(conf, fn="get", sql=query)

    print("*" * 60)
    print("Data query finished")

    print("*" * 60)
    print("Check duplicates")
    duplicates = df_orig[df_orig.duplicated()]
    num_duplicates = len(duplicates)

    print(
        "Original sample size is {}\nNumber of duplicates is {}".format(
            df_orig.shape[0], num_duplicates
        )
    )

    # ensure date columns data type
    date_columns = df_orig.columns[df_orig.columns.str.contains('date')].to_list()
    df_orig[date_columns] = df_orig[date_columns].apply(pd.to_datetime, errors='coerce')

    # Generate PyArrow schema based on DataFrame data types
    new_schema = pa.Schema.from_pandas(df_orig)
    # save data to GS bucket
    save_data(new_schema, conf.model_gs, "pa_schema", conf.bucket_name)
    save_data(
        df_orig, conf.data_gs, "modelling_data", conf.bucket_name, "parquet", new_schema
    )
    print("*" * 60)
    print("Data query successfully")
    return df_orig


def main_data(run_dbt=True, final_model=True, s_number=None):
    # Load configuration
    confparam_path = cur_path.joinpath("src", "conf", "conf_dev.yml")
    dataparam_path = cur_path.joinpath("src", "data", "dbt_model", "dbt_project.yml")
    # set up your s_number
    conf = Conf(confparam_path, dataparam_path, s_number)

    project_start_time = time.time()
    # run DBT model
    run_data_model(conf, run_dbt=run_dbt, final_model=final_model)
    function_start_time = time.time()
    # query data
    df_orig = query_and_save_data(conf)
    elapsed_time("Query data and save locally", project_start_time, function_start_time)


# %% Run main
if __name__ == "__main__":
    s_number = input(
        "Your s_number (e.g., sxxxxxx, otherwise s_number will be None): "
    ).lower()
    # Check if s_number starts with 's'
    if not s_number.startswith("s"):
        s_number = None
    run_dbt = input("Whether to run DBT model (e.g., True, False): ").lower() == "true"
    if run_dbt:
        final_model = (
            input(
                "True for only run final model or False for run all tables(e.g., True, False): "
            ).lower()
            == "true"
        )
    else:
        final_model = False
    main_data(run_dbt=run_dbt, final_model=final_model, s_number=s_number)
