# Imports
from datetime import date, datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def avse_pred(
    y,
    y_pred,
    X=None,
    exposure_weights=None,
    graph_groups=20,
    where_condition=None,
    return_data=False,
):
    """
    Plots the actual and predicted values of a model against the predicted
    value percentile bands

    Parameters:
        y (Pandas.Series): Contains the actual target values
        y_pred (Pandas.series): Contains the predicted target values
        X (Pandas.DataFrame): Table of predictors, only needed if where_condition exists
        exposure_weights (Pandas.Series): Contains the exposure weights. Default is even exposure
                                          if no exposure weights are provided.
        graph_groups (Int): For continous primary predictors specifies the number of groups to plot. Default: 20
        where_condition (list of str): condition to subset rows - default None, form: ['col1 >= 1', 'col3 >= 1']
        return_data (binary): whether to return the data used to plot the AvsE pred

    Returns:
        plot_data_agg (Pandas.DataFrame): Contains the plotting data and only returned if return_data is true.
    """

    # Check input types
    if not (isinstance(X, pd.DataFrame) or X is None):
        raise TypeError("X is not the correct type")
    if not (isinstance(y, pd.Series) and pd.api.types.is_numeric_dtype(y)):
        raise TypeError("y is not the correct type")
    if not (isinstance(y_pred, pd.Series) and pd.api.types.is_numeric_dtype(y_pred)):
        raise TypeError("y is not the correct type")
    if not (
        isinstance(exposure_weights, pd.Series)
        and pd.api.types.is_numeric_dtype(exposure_weights)
        or exposure_weights is None
    ):
        raise TypeError("sample_weights is not the correct type")
    if not (isinstance(graph_groups, int)):
        raise TypeError("graph_groups is not the correct type")
    if not (isinstance(where_condition, list) or where_condition is None):
        raise TypeError("where_condition is not the correct type")
    if not (isinstance(return_data, bool)):
        raise TypeError("return_data is not the correct type")

    # Check if X exists
    if X is None:
        X = pd.DataFrame([0] * len(y))  # define dummy predictor
    # Check if y is appropriate
    if len(y) != len(X.index):
        raise ValueError("y is not the same length as X")
    # Check if y_pred is appropriate
    if len(y_pred) != len(y.index):
        raise ValueError("y_pred is not the same length as y")
    # Check if sample_weights is appropriate
    if exposure_weights is None:
        exposure_weights = pd.Series([1] * len(X.index))
        exposure_weights.index = y.index
    elif len(exposure_weights) != len(y.index):
        raise ValueError("sample_weights is not the same length as y")
    # Check length of data
    if len(X.index) <= 1:
        raise ValueError(
            "Input data does not exist, please check the X, y, and y_pred datasets"
        )
    # Subset rows dependent on where condition
    if where_condition is not None:
        use_idx = (X.query("&".join(where_condition))).index
    else:
        use_idx = y.index

    # # Remove rows with missing target value
    # predictor_table = predictor_table[~predictor_table['target'].isnull()] #
    # ADD INTO use_idx

    plot_data = pd.DataFrame(
        {
            "actual": y[use_idx],
            "expected": y_pred[use_idx],
            "weights": exposure_weights[use_idx],
        }
    )

    # plot_data = plot_data.sort_values(by=['expected'])
    plot_data.sort_values(by=["expected"], inplace=True)

    # Calculate cumulative weight and bin data
    plot_data["cumulative_exposure_weight"] = plot_data["weights"].cumsum()
    plot_data["bins"] = pd.qcut(
        plot_data["cumulative_exposure_weight"],
        q=graph_groups,
        labels=np.linspace(100 / graph_groups, 100, graph_groups).tolist(),
    )

    aggregate_dictionary = {
        "actual": [lambda x: np.average(x, weights=plot_data.loc[x.index, "weights"])],
        "expected": [
            lambda x: np.average(x, weights=plot_data.loc[x.index, "weights"])
        ],
        "weights": [sum],
    }

    plot_data_agg = plot_data.groupby(plot_data["bins"]).agg(aggregate_dictionary)

    # Construct AvsE predictor plot
    fig, ax = plt.subplots()
    ax2 = ax.twinx()
    lns1 = ax.plot(
        plot_data_agg.index,
        plot_data_agg["actual"],
        "b-",
        label="Actual Target",
        marker="x",
    )
    lns2 = ax.plot(
        plot_data_agg.index,
        plot_data_agg["expected"],
        "r-",
        label="Predicted Target",
        marker="x",
    )
    lns3 = ax2.bar(
        np.squeeze(plot_data_agg.index.values),
        np.squeeze(plot_data_agg["weights"].values),
        label="Total Weight",
        color="gray",
        width=25 / graph_groups * 3.5,
        alpha=0.25,
    )

    # Plot text
    fig.legend(bbox_to_anchor=(0.5, -0.04), loc="lower center", ncol=3, frameon=False)
    figtext = (
        "Actual: {actual} | Current predicted: {pred} | No. of obs: {obs} \n"
        "Date: {date} | Time: {time} \n Where: {where} | Weight: {weight} ".format(
            actual=y.name,
            pred=y_pred.name,
            obs=len(X.index),
            date=date.today(),
            time=datetime.now().strftime("%H:%M:%S"),
            where=where_condition,
            weight=exposure_weights.name,
        )
    )
    plt.figtext(0.5, -0.1, figtext, ha="center")

    # Plot axes
    ax.set_xlim([0, 100])
    ax.set_xlabel("Predicted Percentile Band")
    ax.set_ylabel("Mean Value")
    ax2.set_ylabel("Total Weight")
    ax.set_title("AvsE by Predicted Band")
    plt.tight_layout(pad=1.4)

    # Show plot
    ax.set_zorder(1)  # default zorder is 0 for ax1 and ax2
    ax.patch.set_visible(False)  # prevents ax1 from hiding ax2
    # plt.show()

    if return_data:
        return (fig, plot_data_agg)
    else:
        return fig
