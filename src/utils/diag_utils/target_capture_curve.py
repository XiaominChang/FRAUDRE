# Imports
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score


def target_capture_curve(df, pred, targets, where_condition=None):

    if where_condition is None:
        use_idx = df.index.values
    else:
        use_idx = (df.query(where_condition)).index.values

    thresholds = np.linspace(0, 1, 20)

    summary = pd.DataFrame()

    df_use = df.loc[use_idx]
    df_use["count"] = 1

    totals = df_use[targets + ["count"]].sum().reset_index(name="total")
    for threshold in thresholds:
        df_selected = df_use.sort_values(pred, ascending=False).head(
            round(threshold * len(df_use))
        )
        summary_thresh_1 = (
            df_selected[targets + ["count"]].sum().reset_index(name="amount_flagged")
        )
        summary_thresh = summary_thresh_1.merge(totals, on="index")
        summary_thresh["proportion"] = (
            summary_thresh["amount_flagged"] / summary_thresh["total"]
        )
        summary_thresh["proportion_claims"] = summary_thresh[
            summary_thresh["index"] == "count"
        ].iloc[0, 3]
        summary = pd.concat([summary, summary_thresh], ignore_index=True)

    # Calculate AUC and add new index (only for binary targets)
    binary_targets = (
        True
        if np.max([len(df_use[target].unique()) for target in targets]) < 3
        else False
    )
    if binary_targets:
        try:
            auc = [
                str(round(roc_auc_score(df_use[target], df_use[pred]), 4))
                for target in targets
            ]
            labels = [
                (pair[0], ": ".join(pair))
                for pair in [[i, j] for i, j in zip(targets, auc)]
            ]
            labels.insert(0, ("count", "count: 0.5000"))
            for index, label in labels:
                summary.loc[summary["index"] == index, "index"] = label
        except Exception as error:
            print("Failed to calculate AUC for binary targets with error")
            print(error)

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    ax.set_xlabel("Total Proportion of Claims")
    ax.set_ylabel("Total Proportion of Target")

    ax.set_title("Gains - Multiple Targets")

    summary.set_index("proportion_claims", inplace=True)
    summary.groupby("index")["proportion"].plot(legend=True)
