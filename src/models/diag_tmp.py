import base64
import sys
from datetime import datetime
from io import BytesIO

import matplotlib.pyplot as plt
import pathlib
import seaborn as sns
from sklearn.metrics import classification_report, confusion_matrix, recall_score

# add working dir to the path
cur_path = pathlib.Path(__file__).resolve().parent.parent.parent.absolute()
src_loc = cur_path.joinpath("src")
sys.path.append(str(cur_path))
sys.path.append(str(src_loc))
# Import custom functions or utilities
from src.conf import Conf
from src.utils.utils import load_data

# Initialize SHAP for visualization


# Load configuration
confparam_path = cur_path.joinpath("src", "conf", "conf_dev.yml")
dataparam_path = cur_path.joinpath("src", "data", "MAD_model", "dbt_project.yml")
conf = Conf(confparam_path, dataparam_path)  # add s_number as parameter
# Read in model data
diag_file = load_data(conf.model_gs, "diagnostics_data", None)
doc_title = "Diagnostics for " + diag_file["model_name"]

# Confusion Matrices - Train
threshold = 0.1
y_pred_01 = diag_file["y_pred_ave"].map(lambda x: 1.0 if x > threshold else 0.0)


recall = recall_score(diag_file["y_ave"], y_pred_01)
classification_rep = classification_report(diag_file["y_ave"], y_pred_01)
cf_matrix = confusion_matrix(diag_file["y_ave"], y_pred_01)

# Plot confusion matrix
plt.figure()
sns.heatmap(cf_matrix, annot=True, cmap="Blues")
plt.xlabel("Predicted label")
plt.ylabel("True label")
plt.title("Confusion Matrix")

# Save the plot to a BytesIO object
buffer = BytesIO()
plt.savefig(buffer, format="png")
buffer.seek(0)

# Convert the plot to a base64 encoded string
image_base64 = base64.b64encode(buffer.read()).decode("utf-8")

# Generate HTML content
html_content = f"""
<html>
<head>
    <title>Model Diagnostics</title>
</head>
<body>
    <h1>Model Diagnostics</h1>
    <p>Date: {datetime.now().strftime('%Y-%m-%d')}</p>

    <h2>Metrics</h2>
    <p>Recall score: {recall}</p>

    <h2>Classification Report</h2>
    <pre>{classification_rep}</pre>

    <h2>Confusion Matrix</h2>
    <img src="data:image/png;base64, {image_base64}" alt="Confusion Matrix">
</body>
</html>
"""

# Save HTML content to file
with open("report.html", "w") as f:
    f.write(html_content)
