# config for data, model and GCP related
import os
import subprocess

import pathlib
import yaml
from dotenv import load_dotenv


class Conf:
    """Conf Class to store all the application parameters."""

    def __init__(self, confparam_path: str, dataparam_path: str, s_number=None):
        self.confparam_path = confparam_path
        self.dataparam_path = dataparam_path

        # Get the directory containing the current script (conf.py)
        cur_script_dir = pathlib.Path(__file__).resolve()
        # Navigate up two levels to get the parent directory
        self.cur_path = cur_script_dir.parent.parent.absolute()
        self.src_loc = self.cur_path.joinpath("src")
        self.confparam = self.load_conf(confparam_path)
        self.dataparam = self.load_conf(dataparam_path)
        self.root = self.confparam.get("dev_config", {})
        self.db = self.root.get("data").get("db")
        self.database = self.root.get("data").get("database")
        self.host = self.root.get("data").get("host")
        self.port = self.root.get("data").get("port")
        self.schema = self.root.get("data").get("schema")
        self.type = self.root.get("data").get("type")
        self.output_db = self.root.get("score").get("output-db")
        self.output_database = self.root.get("score").get("output-database")
        self.id_features = self.root.get("feature").get("id_features", [])
        self.num_features = self.root.get("feature").get("num_features", [])
        self.bnr_features = self.root.get("feature").get("bnr_features", [])
        self.ord_features = self.root.get("feature").get("ord_features", [])
        self.ohe_features = self.root.get("feature").get("ohe_features", [])
        self.diagnostics_path = os.path.join(self.cur_path, "models", "diagnostics")
        self.data_path = os.path.join(self.cur_path, "data")
        self.models_path = os.path.join(self.cur_path, "models")
        self.artefact_path = os.path.join(self.cur_path, "models")
        self.target_feature = self.root.get("model").get("target_feature", [])
        self.condition_policy = self.root.get("model").get("condition_policy", [])
        self.condition_start = self.root.get("model").get("condition_start")
        self.condition_end = self.root.get("model").get("condition_end")
        self.model_start_date = self.root.get("model").get("model_start_date")
        self.model_end_date = self.root.get("model").get("model_end_date")
        self.fut_start_date = self.root.get("model").get("fut_start_date")
        self.fut_end_date = self.root.get("model").get("fut_end_date")
        # load local config
        if s_number is None:
            load_dotenv()
            self.s_number = None
            # self.secret_data = os.getenv("secret_key")
            self.secret_data = self.cur_path
            self.bucket_name = None
            self.data_gs = self.data_path
            self.model_gs = self.models_path
            self.diag_gs = self.models_path
            self.stag_gs = self.models_path
        # local GCP config
        else:
            self.s_number = s_number  # New parameter
            self.project_id = "dia-hkn-5d10"
            self.region = "australia-southeast1"
            self.user_secret_id = f"aap-prod-{self.s_number}-secret"
            self.bucket_name = f"dia-hkn-5d10-aap-prod-{self.s_number}-bucket"
            self.project_name = "aai-modelling-temp"  # set to your project name
            self.kms_key_name = (
                f"projects/{self.project_id}/locations/{self.region}/"
                f"keyRings/aap-datalab-users-keyring/cryptoKeys/aap-prod-{self.s_number}-key"
            )
            self.secret_data = self._get_secret_data()
            self.data_gs = os.path.join(self.project_name, "data")
            self.model_gs = os.path.join(self.project_name, "models")
            self.diag_gs = os.path.join(self.project_name, "diagnostics")
            self.stag_gs = os.path.join(self.project_name, "temp")

    def load_conf(self, conf_path):
        try:
            with open(conf_path, "r") as conf_file:
                conf_data = yaml.load(conf_file, Loader=yaml.FullLoader)
            return conf_data
        except FileNotFoundError:
            print(f"Configuration file '{conf_path} not found.")
            return {}

    def _get_secret_data(self):
        # Construct the gcloud command to access the latest version of the
        # secret
        command = (
            f"gcloud secrets versions access latest --secret={self.user_secret_id}"
        )

        try:
            # Execute the command and capture its output with a timeout of 30
            # seconds
            completed_process = subprocess.run(
                command, shell=True, text=True, capture_output=True, timeout=30
            )
            # Check if the command was successful
            if completed_process.returncode == 0:
                output = completed_process.stdout
                # Print or process the output as needed
                return output.strip()
            # Handle the case when the command returns a non-zero exit code
            print(f"Error accessing secret: {completed_process.stderr}")
        except subprocess.TimeoutExpired:
            # Handle the case when the subprocess execution times out
            print("Subprocess execution timed out.")
        except Exception as ex:
            # Handle any other exceptions that might occur
            print(f"An unexpected error occurred: {ex}")
        return None
