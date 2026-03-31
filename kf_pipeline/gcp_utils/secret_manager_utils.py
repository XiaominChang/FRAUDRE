
# -- -------------------------------------------------------------------------
# -- Author:      Behzad Asadi                                                                                  
# -- Description: Utility functions for working with Google Secret Manager   
# -- -------------------------------------------------------------------------
# -- VERSIONS       DATE            WHO                      DESCRIPTION                                                
# -- 1.00           02/04/2024      Behzad Asadi             Initial release        
# -- -------------------------------------------------------------------------


#%% importing libraries

import os
import json
from google.cloud import secretmanager


#%% saving user credentials in secret manager

S_NUMBER="s125439"
AAP_ENV="prod"
PROJECT_ID="dia-clm-7642"
REGION="australia-southeast1"
USER_SECRET_ID=f"aap-prod-{S_NUMBER}-secret"

client = secretmanager.SecretManagerServiceClient()
parent = client.secret_path(PROJECT_ID, USER_SECRET_ID)

path_in_str = "./user_secrets.json"

with open(path_in_str) as file:

    #  load data into variable
    creds = json.load(file)
    file.close()

creds = str(creds)
creds_bytes = creds.encode("UTF-8")

response = client.add_secret_version(
        request={
            "parent": parent,
            "payload": {
                "data": creds_bytes,
            },
        }
)


#%% saving system account credentials in secret manager

AAP_ENV="prod"
PROJECT_ID="dia-clm-7642"
REGION="australia-southeast1"
USER_SECRET_ID=f"aap-prod-model-secret"

client = secretmanager.SecretManagerServiceClient()
parent = client.secret_path(PROJECT_ID, USER_SECRET_ID)

path_in_str = "./system_secrets.json"

with open(path_in_str) as file:

    #  load data into variable
    creds = json.load(file)
    file.close()

creds = str(creds)
creds_bytes = creds.encode("UTF-8")

response = client.add_secret_version(
        request={
            "parent": parent,
            "payload": {
                "data": creds_bytes,
            },
        }
)


#%% accessing credentials from secret manager

#client = secretmanager.SecretManagerServiceClient()
#secret_data = client.access_secret_version(name=f"projects/{PROJECT_ID}/secrets/{USER_SECRET_ID}/versions/latest").payload.data.decode('UTF-8')
