
# -- -------------------------------------------------------------------------
# -- Author:      Behzad Asadi                                                                                  
# -- Description: Creating config for Vertex AI Jobs   
# -- -------------------------------------------------------------------------
# -- VERSIONS       DATE            WHO                      DESCRIPTION                                                
# -- 1.00           02/04/2024      Behzad Asadi             Initial release        
# -- -------------------------------------------------------------------------

#%% Importing libraries

import os
from pathlib import Path
import pytz
from datetime import datetime

#%% model package parameters

def config(principle, model_id, s_number = None):
    
    working_dir = Path(__file__).parents[1]
    
    # monitoring package parameters
    monitor_python_package_name=f"model_monitoring"
    monitor_python_package_version="0.0.1"
    monitor_python_package_local_file_path = os.path.join(working_dir, f"monitor_package/dist", f"{monitor_python_package_name}-{monitor_python_package_version}.tar.gz")
    monitor_python_package_gcs_blob_name = f"{model_id}/code/package" + "/" + monitor_python_package_name + "/" + f"{monitor_python_package_name}-{monitor_python_package_version}.tar.gz"
    monitor_report_gcs_path = f"{model_id}/monitor/{monitor_python_package_version}"
    monitor_customjob_display_name = f"{model_id}-model-monitoring"
    monitor_customjob_base_image="asia-docker.pkg.dev/vertex-ai/training/sklearn-cpu.1-6:latest"

    # model package parameters
    model_python_package_name="model_scoring"
    model_python_package_version="0.0.1"
    model_python_package_local_file_path = os.path.join(working_dir, f"model_package/dist", f"{model_python_package_name}-{model_python_package_version}.tar.gz")
    model_python_package_gcs_blob_name = f"{model_id}/code/package" + "/" + model_python_package_name + "/" + f"{model_python_package_name}-{model_python_package_version}.tar.gz"
    model_customjob_display_name = f"{model_id}-model-scoring"
    model_customjob_base_image="asia-docker.pkg.dev/vertex-ai/training/sklearn-cpu.1-6:latest"
    
    # Vertex AI Dataset parameters
    training_data_local_file_path = os.path.join(working_dir, f"kf_pipeline_scoring/data", f"training_data.csv")
    training_data_gcs_blob_name=f"{model_id}/data/training/training_data.csv"
    dataset_display_name = f"{model_id}-dataset-training"

    # pipeline parameters
    pipeline_job_display_name = f"{model_id}"
    pipeline_file_name = f"{pipeline_job_display_name}-pipeline.json"
    pipeline_gcs_blob_name = f"{model_id}/pipelines/"+ pipeline_file_name
    pipeline_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    pipeline_job_id = f"{pipeline_job_display_name}-{pipeline_timestamp}"

    config_dict= {
        "monitor_python_package_name" : monitor_python_package_name,
        "monitor_python_package_version" : monitor_python_package_version,
        "monitor_python_package_local_file_path" : monitor_python_package_local_file_path,
        "monitor_python_package_gcs_blob_name" : monitor_python_package_gcs_blob_name,
        "monitor_report_gcs_path" : monitor_report_gcs_path,
        "monitor_customjob_display_name" : monitor_customjob_display_name,
        "monitor_customjob_base_image" : monitor_customjob_base_image,
        "model_python_package_name" : model_python_package_name,
        "model_python_package_version" : model_python_package_version,
        "model_python_package_local_file_path" : model_python_package_local_file_path,
        "model_python_package_gcs_blob_name" : model_python_package_gcs_blob_name,
        "model_customjob_display_name" : model_customjob_display_name,
        "model_customjob_base_image" : model_customjob_base_image,
        "training_data_local_file_path" : training_data_local_file_path,
        "training_data_gcs_blob_name" : training_data_gcs_blob_name,
        "dataset_display_name" : dataset_display_name,
        "pipeline_file_name": pipeline_file_name,
        "pipeline_gcs_blob_name" : pipeline_gcs_blob_name,
        "pipeline_timestamp": pipeline_timestamp, 
        "pipeline_job_display_name" : pipeline_job_display_name,
        "pipeline_job_id": pipeline_job_id
    }


    if principle == "user" and s_number is not None:
        
        # GCP parameters 
        aap_env="prod"
        project_id="dia-frd-0586"
        region="australia-southeast1"
        user_secret_id=f"aap-prod-{s_number}-secret"
        bucket_name=f"{project_id}-aap-{aap_env}-{s_number}-bucket"
        bucket_uri="gs://" + bucket_name
        key_ring="aap-datalab-users-keyring"
        cmek=f"projects/{project_id}/locations/{region}/keyRings/{key_ring}/cryptoKeys/aap-{aap_env}-{s_number}-key"
        service_account=f"aap-{aap_env}-{s_number}-sa@{project_id}.iam.gserviceaccount.com"
        iag_network="projects/996702533063/global/networks/vpc-prod-shared-base"
        
        # monitor package parameters
        monitor_python_package_gcs_uri= f"{bucket_uri}/"+ monitor_python_package_gcs_blob_name
        monitor_customjob_staging_gcs_uri=f"{bucket_uri}/{model_id}/temp"

        # model package parameters
        model_python_package_gcs_uri= f"{bucket_uri}/"+ model_python_package_gcs_blob_name
        model_customjob_staging_gcs_uri=f"{bucket_uri}/{model_id}/temp"

        # pipeline parameters
        pipeline_gcs_root = f"{bucket_uri}/{model_id}/pipelines"
        pipeline_gcs_uri = f"{bucket_uri}/"+ pipeline_gcs_blob_name


        user_dict = {
            "s_number": s_number,
            "aap_env" : aap_env,
            "project_id" : project_id,
            "region" : region,
            "user_secret_id" : user_secret_id,
            "bucket_name" : bucket_name,
            "bucket_uri" : bucket_uri,
            "key_ring" : key_ring,
            "cmek" : cmek,
            "service_account" : service_account,
            "iag_network" : iag_network,
            "monitor_python_package_gcs_uri" : monitor_python_package_gcs_uri,
            "monitor_customjob_staging_gcs_uri" : monitor_customjob_staging_gcs_uri,
            "model_python_package_gcs_uri" : model_python_package_gcs_uri,
            "model_customjob_staging_gcs_uri" : model_customjob_staging_gcs_uri,
            "pipeline_gcs_root" : pipeline_gcs_root,
            "pipeline_gcs_uri": pipeline_gcs_uri
        }
        config_dict.update(user_dict)

    elif principle == "service":
        
        # GCP parameters 
        aap_env="prod"
        project_id="dia-frd-0586"
        region="australia-southeast1"
        user_secret_id=f"aap-prod-model-secret"
        bucket_name=f"{project_id}-aap-{aap_env}-model-bucket"
        bucket_uri="gs://" + bucket_name
        key_ring="aap-datalab-users-keyring"
        cmek= cmek=f"projects/{project_id}/locations/{region}/keyRings/{key_ring}/cryptoKeys/aap-{aap_env}-model-key"
        service_account="aap-prod-model-sa@dia-frd-0586.iam.gserviceaccount.com"
        iag_network="projects/996702533063/global/networks/vpc-prod-shared-base"
        
        # monitor package parameters
        monitor_python_package_gcs_uri= f"{bucket_uri}/"+ monitor_python_package_gcs_blob_name
        monitor_customjob_staging_gcs_uri=f"{bucket_uri}/{model_id}/temp"

        # model package parameters
        model_python_package_gcs_uri= f"{bucket_uri}/"+ model_python_package_gcs_blob_name
        model_customjob_staging_gcs_uri=f"{bucket_uri}/{model_id}/temp"

        # pipeline parameters
        pipeline_gcs_root = f"{bucket_uri}/{model_id}/pipelines"
        pipeline_gcs_uri = f"{bucket_uri}/"+ pipeline_gcs_blob_name
        
        service_dict = {
            "aap_env" : aap_env,
            "project_id" : project_id,
            "region" : region,
            "user_secret_id" : user_secret_id,
            "bucket_name" : bucket_name,
            "bucket_uri" : bucket_uri,
            "key_ring" : key_ring,
            "cmek" : cmek,
            "service_account" : service_account,
            "iag_network" : iag_network,
            "monitor_python_package_gcs_uri" : monitor_python_package_gcs_uri,
            "monitor_customjob_staging_gcs_uri" : monitor_customjob_staging_gcs_uri,
            "model_python_package_gcs_uri" : model_python_package_gcs_uri,
            "model_customjob_staging_gcs_uri" : model_customjob_staging_gcs_uri,
            "pipeline_gcs_root" : pipeline_gcs_root,
            "pipeline_gcs_uri": pipeline_gcs_uri
        }
        config_dict.update(service_dict)
    else:
        raise ValueError(f"either Value {principle} for principle is not supported (please use 'user' or 'service') or s_number is None.")
    
    return config_dict

#%%


if __name__ == '__main__':
    
    params_user = config(principle='user', model_id='amm-395',s_number ='s123815')
    params_service = config(principle='service', model_id='amm-395')




# %%
