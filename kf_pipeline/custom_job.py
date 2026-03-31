# %%

# -- -------------------------------------------------------------------------
# -- Author:      Behzad Asadi                                                                                  
# -- Description: Vertex AI Custom Job   
# -- -------------------------------------------------------------------------
# -- VERSIONS       DATE            WHO                      DESCRIPTION                                                
# -- 1.00           05/04/2024      Behzad Asadi             Initial release        
# -- -------------------------------------------------------------------------

# %% importing libraries

import os
import subprocess
from datetime import datetime
from google.cloud import storage
from google.cloud import aiplatform
from gcp_utils import list_blobs
from config import config

# %% parameters

#params = config(principle='user', model_id='amm-344',s_number ='s125591')
params = config(principle='service', model_id='amm-363')

# %% defining and running the dbt job

# dbt_custom_job = aiplatform.CustomPythonPackageTrainingJob(display_name=params["dbt_customjob_display_name"],
#                                                     project=params["project_id"],
#                                                     location=params["region"],
#                                                     python_package_gcs_uri=params["dbt_python_package_gcs_uri"], 
#                                                     python_module_name=params["dbt_python_package_name"]+'.task', 
#                                                     container_uri=params["dbt_customjob_base_image"],
#                                                     staging_bucket=params["dbt_customjob_staging_gcs_uri"],
#                                                     training_encryption_spec_key_name=params["cmek"],
#                                                     model_encryption_spec_key_name=params["cmek"]
#                                                    )

# dbt_custom_job.run(
#     replica_count=1,
#     machine_type="n1-standard-4",
#     service_account = params["service_account"],
#     network= params["iag_network"],   
# )

# # %% defining and running the model job

# model_custom_job = aiplatform.CustomPythonPackageTrainingJob(display_name=params["model_customjob_display_name"],
#                                                       project=params["project_id"],
#                                                       location=params["region"],
#                                                       python_package_gcs_uri=params["model_python_package_gcs_uri"], 
#                                                       python_module_name=params["model_python_package_name"]+'.task', 
#                                                       container_uri=params["model_customjob_base_image"],
#                                                       staging_bucket=params["model_customjob_staging_gcs_uri"],
#                                                       training_encryption_spec_key_name=params["cmek"],
#                                                       model_encryption_spec_key_name=params["cmek"]
#                                                      )

# model_custom_job.run(
#     replica_count=1,
#     machine_type="n2-highmem-8",
#     service_account = params["service_account"],
#     network= params["iag_network"],   
# )





pipeline_job = aiplatform.PipelineJob(
    display_name=params['pipeline_job_display_name'],
    template_path=params['pipeline_file_name'],
    pipeline_root=params['pipeline_gcs_root'],
    job_id=params['pipeline_job_id'],
    enable_caching=False,
    encryption_spec_key_name=params['cmek'],
    labels={"org":"dia"},
    location=params['region'],
    project=params['project_id'],
    parameter_values={
        'model_python_package_name': params['model_python_package_name'], 
        'model_python_package_gcs_blob_name': params['model_python_package_gcs_blob_name'],
        'bucket_name': params['bucket_name']
    },
)


pipeline_job.submit(service_account=params['service_account'], network=params['iag_network'])



# %%
