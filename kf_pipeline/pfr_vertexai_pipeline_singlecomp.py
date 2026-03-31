# %%

# -- -------------------------------------------------------------------------
# -- Author:      Behzad Asadi                                                                                  
# -- Description: Vertex AI Pipeline   
# -- -------------------------------------------------------------------------
# -- VERSIONS     DATE            WHO                        DESCRIPTION                                                
# -- 1.00        05/04/2024     Behzad Asadi                Initial release        
# -- -------------------------------------------------------------------------

# Alternative approaches:
#    - CustomTrainingJobOp
#    - create_custom_training_job_from_component

# %% importing libraries

import os
import subprocess
from datetime import datetime
from gcp_utils import download_blob, upload_blob, list_blobs
from config import config
from google.cloud import storage
from google.cloud import secretmanager
from google.cloud import aiplatform
from kfp.v2 import compiler, dsl
from kfp.v2.dsl import component, pipeline, Artifact, ClassificationMetrics, Input, Output, Model, Metrics

# %% Utilities

#params = config(principle='user', s_number='s125591', model_id='amm-363')
params = config(principle='service', model_id='amm-363')

# %% defining components

@component(base_image=params['model_customjob_base_image'])
def model_scoring(model_python_package_name: str, model_python_package_gcs_blob_name: str, bucket_name: str):
    
    import os
    import subprocess
    import importlib
    from google.cloud import storage
    from google.cloud import aiplatform
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(model_python_package_gcs_blob_name)
    blob.download_to_filename('./'+os.path.basename(model_python_package_gcs_blob_name))
    
    # install model_scoring package
    subprocess.check_call(['pip', 'install', os.path.basename(model_python_package_gcs_blob_name)])
    
    # isntall packages required by model_scoring
    model_module = importlib.import_module(f'{model_python_package_name}.task')
    model_module.scoring_main()


# %% defining and running the job

@pipeline(
    name= "amm-344 pipeline",
    description="A pipeline to create the data model, and run the scoring code",
)

def model_pipeline(model_python_package_name: str, 
                   model_python_package_gcs_blob_name: str,
                   bucket_name: str):
    
    
    model_scoring_op = model_scoring(model_python_package_name=model_python_package_name, 
                   model_python_package_gcs_blob_name=model_python_package_gcs_blob_name, 
                   bucket_name=bucket_name) \
                    .set_cpu_limit('8') \
                    .set_memory_limit('64G')


compiler.Compiler().compile(
    pipeline_func=model_pipeline, package_path=params["pipeline_file_name"]
)

subprocess.check_call(["sed", "-i", "s/--quiet/--quiet --index-url https:\/\/nexus3.auiag.corp\/repos\/repository\/ddo-pypi\/simple --trusted-host nexus3.auiag.corp/g", params["pipeline_file_name"]])

upload_blob(params['bucket_name'], params["pipeline_file_name"], params["pipeline_gcs_blob_name"])

# %%

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

# %% scheduling the job

# pipeline_schedule = pipeline_job.create_schedule(
#     cron="00 2 * * *",  # Run daily at 06:00 UTC, which is 17:00 AEDT
#     display_name=params['pipeline_job_display_name']+"-daily-schedule",
#     start_time="2024-04-04T00:00:00Z",  # Start date 
#     service_account=params['service_account'],
#     network=params['iag_network'],
# )

# pipeline_schedule.submit()


# %%
