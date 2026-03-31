# Template code for model training CFMS team
## Owners: Derek Ding, Reza Mohajerpoor
## Developers: Derek Ding, Avlok Bahri, Clara Chun, Reza Mohajerpoor 
### April 2024
> This is the template code used across A&AI team for ML model trainings, from classification to regression, supervised or unsupervised learnings
> Main branch is template for light GBM, isolation-forest branch is for unsupervised learning
> If want to use isolation-forest branch, please select `Include all branches` when create a new repository use this template, and switch to isolation-forest branch 
> The code can run on local systems, or on GCP depending on the value of s_number

## Structure of the repository
    ├── .venv                          - a gitignored folder to create the virtual envs
    ├── Data                           - Folder for storing data locally
    ├── dependencies                   - Folder for storing packages to be installed 
    ├── models                         - Folder for storing models and evaluation artefacts
    ├── notebooks                      - Folder for storing Jupyter Notebooks used for model development
    ├── src                            - Source code for use in this project
    │   ├── train.py                   - Script to run model training pipeline from end to end, will call src.data,src.eda,src.models modules in sequence
    │   ├── install_packages.sh        - Shell script to install packages when creating a source distribution package
    │   ├── conf.py                    - Script to store basic configuration settings that are used throughout the project
    │   ├── cert                       - Folder for storing certificate when packaging scripts
    │   ├── conf                       - Folder conatins yml file to store data and model info
    │   ├── data                       - Module for data query including DBT model
    │   ├── eda                        - Module for data processing
    │   ├── features                   - Placeholder for data engineering
    │   ├── models                     - Module for model training as well as diagnostic scripts
    │   ├── utils                      - Module for functions that are used throughout the project, including data saving, loading, query, feature names generating
    │   │   ├── model_utils            - Module for hash split, model trainer and evaluator
    │   │   ├── diag_utils             - Module for model diagnostic report
    ├── tests                          - Folder for tests to be used when deploy
    ├── README.md                      - Descriptions and model specific information
    ├── requirements_dev.txt           - File contains a list of packages required for development purposes
    ├── pyproject.toml                 - This file contains project metadata and configuration for the `poetry` dependency manager
    ├── setup.py                       - Script to define the project's metadata and provide information for packaging and distribution for Vertex AI custom training
    ├── MANIFEST.in                    - File to specify additional files or directories that should be included when creating a source distribution package by setup.py
    ├── .gitignore                     - File to exclude any path or file type to be pushed 

## Use Conf object
In the `src.conf` module, the Conf class object is utilized to manage fundamental configuration settings crucial for the entire project. It defines:

- When `s_number` is None: Data and model-related information is loaded for usage within a local VM environment. Additionally, it loads the .env file to facilitate interaction with EDH. Paths are defined for saving and loading artifacts.

- When `s_number` is an actual value: Relative information is loaded for use on Google Cloud Platform (GCP), facilitating interaction with the GCS bucket. Furthermore, the secret key is loaded from the secret manager to enable interaction with EDH.


## Initial setup
To query data from EDH, `src.utils.sql` need username and password to connect
Follow the instructions to genearte `JSON` format `secret_key`:

- Generate secret key on the Vertex AI workbench

        from google.cloud import secretmanager
        PROJECT_ID="dia-xxx-xxxx"
        REGION="australia-xxxxxx"
        USER_SECRET_ID="aap-xxx-sxxxxxx-secret"

        client = secretmanager.SecretManagerServiceClient()
        parent = client.secret_path(PROJECT_ID, USER_SECRET_ID)

        creds='{"EDH_PROD": { "username": "sxxxxxx", "password": "xxxxxx" }, "EDH_NONPROD": { "username": "sxxxxxx", "password": "xxxxxx" }}'

        creds_bytes = creds.encode("UTF-8")


        response = client.add_secret_version(
                request={
                    "parent": parent,
                    "payload": {
                        "data": creds_bytes,
                    },
                }
        )
- Generate secret key on prem
Environment variables are saved in the `.env` file which can be found in the `home directory`.
`home director` could be parent path of your current project path.
In .env file, add

        secret_key = { "EDH_PROD": { "username": "sxxx", "password": "xxxx" }, "EDH_NONPROD": { "username": "sxxx", "password": "xxxx" } }

### Environment setup
- Create one environment for data modelling and one environment for modelling pipeline
- create .vnev folder (gitignored) in the current repo and install the virtual envs there 

#### Environment setup on laptop 
- **requirements_dev.txt**:
Packages for `pip`:
      - cd to to the .venv folder 
      - virtualenv <your-env>
      - .venv\Scripts\activate
      - pip install -r requirements_dev.txt

- **pyproject.toml**: 
Packages for `poetry`:

      poetry install --no-root --file pyproject.toml
      --no-root: This flag prevents Poetry from installing the project itself as a dependency.
      --file pyproject.toml: This specifies the file containing the project dependencies.

#### Environment setup on VERTEX AI workbench
- **requirements_dev.txt**:
    - cd to to the venv folder
    - virtualenv <your-env>
    - source <venv_loc>/bin/activate
    - pip install -r requirements_dev.txt
    - "python -m ipykernel install --user --name <ENV_NAME>"  to create the kernel
    
          
## Useful codes in gcloud/gsutil

### Login to access Google APIs within your workbench
gcloud auth application-default login
gcloud auth login

### Accessing your bucket in a Workbench
gsutil ls <project-id>-aap-prod-<snumber>-bucket

### Creating and Accessing your secret in a Workbench

gcloud secrets versions add aap-prod-<s_number>-secret --data-file='<secret file name>'
gcloud secrets versions access latest --secret="aap-prod-<s_number>-secret"

    
## GCP GUIDE:
To remove a folder in GCP workbench: 
	rm -r <Folder_name>
To change the environment variables:
	vim ~/.bashrc
	i for insert and changing the env
	export PATH="/home/jupyter/.local/bin:$PATH"
	Esc and :x for saving the files
To create a Kernek for Virtual environment:
	python -m ipykernel install --user --name <ENV NAME>
To remove a Virtual env Kernel in GCP:
	jupyter kernelspec list
	jupyter kernelspec uninstall  <KERNEL to be REMOVED>

## Use your s_number
All scripts in this project are designed for versatility, running seamlessly on both Google Cloud Platform (GCP) and local virtual machines (VMs). The `s_number` parameter determines the environment:

- `None`: Interacts with local folders for easy development/testing.
- Actual `s_number`: Interacts with Google Cloud Storage (GCS), facilitating integration with GCP services.
- When work on GCP, run both `gcloud auth login` and `gcloud auth application-default login` to interact with GCS

This setup ensures adaptability for Vertex AI custom training pipelines, offering flexibility across environments.


## Miscellaneous

### Diagnostics report

- Diagnostics report template is in src/models repo, which is a qmd file
- To run it, need to install quarto both in your virtual env and on local machine
- `poetry` can handle the quarto installment in virtural env
- To install on local machine:

      download from https://quarto.org/docs/download/ cli quarto and install it
      remember installment path and add it to your system env path

### Poetry management

- To install poetry you can use `pip install --index-url https://nexus3.auiag.corp/repos/repository/ddo-pypi/simple --trusted-host nexus3.auiag.corp poetry`
- Ensure that poetry is installed to your base environment on workbench or system wide on VM
- Add path to Vertex Ai workbench from terminal: `export PATH="/home/jupyter/.local/bin:$PATH"`
- Use IAG certificate when run `poetry install`
- Certiciate is located in src/cert or you can download from `https://certstore.iag.com.au/iag_ca_cert_chain.pem`
- Export `REQUESTS_CA_BUNDLE` `SSL_CERT_FILE` `NODE_EXTRA_CA_CERTS` `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH` to the path to iag_ca_cert_chain.pem
- On VM, need to set system environment variables of certificate path

### pyproject.toml file

- pyproject.toml for this project is in the root folder anomaly_detection_motor_GCP/
- the one in the src folder is only for building distribution packages using for custom job training

##  run_data_model.py

- data model codes: need knowledge of dbt
- drop existing tables: make sure the owner of the data tables is a group rather than yourself
- run_data_model: run dbt using subprocess functions
- query_and_save_data: query the data perform data type transformations required, update the control file and save the artefacts


## train.py

- train.py is the script to run model training from end to end
- the best way to test all scripts in this project is to run train.py
- input `s_number` to test whether it can run successfully on Vertex AI workbench
- use `None` as s_number input value to test whether it can run successfully on local VMs