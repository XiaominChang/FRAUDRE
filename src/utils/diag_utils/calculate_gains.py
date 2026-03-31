# Imports
from datetime import date, datetime

import matplotlib.pyplot as plt
import pandas as pd


def calculate_gains(y, y_pred, X=None, exposure_weights=None, where_condition=None):
    """
    Plots a gains curve, and calculates percentage of theoretical max gains acheived by predictions

    Parameters:
        y (pandas.series): vector of target values
        y_pred (pandas.series): vector of prediction values
        sample_weights (pandas.series): vector of weight values
        X (pandas.df): table of predictors, only needed if where_condition exists
        where_condition (list of str): condition to subset rows - default None, form: ['col1 >= 1', 'col3 >= 1']

    Returns:
        returns Gains_ratio, cumulative gains plot
    """

    # Check input types
    if not (isinstance(y, pd.Series) and pd.api.types.is_numeric_dtype(y)):
        raise TypeError("y is not the correct type")
    if not (isinstance(y_pred, pd.Series) and pd.api.types.is_numeric_dtype(y_pred)):
        raise TypeError("y is not the correct type")
    if not (isinstance(X, pd.DataFrame) or X is None):
        raise TypeError("X is not the correct type")
    if not (
        isinstance(exposure_weights, pd.Series)
        and pd.api.types.is_numeric_dtype(exposure_weights)
        or exposure_weights is None
    ):
        raise TypeError("sample_weights is not the correct type")
    if not (isinstance(where_condition, list) or where_condition is None):
        raise TypeError("where_condition is not the correct type")

    # Check if X exists
    if X is None:
        dummy_predictor = [1] * len(y)
        X = pd.DataFrame(dummy_predictor)
        X.index = y.index

    # Check if y is appropriate
    if len(y) != len(X.index):
        raise ValueError("y is not the same length as X")

    # Check if y_pred is appropriate
    if len(y_pred) != len(X.index):
        raise ValueError("y_pred is not the same length as X")

    # Check if sample_weights is appropriate
    if exposure_weights is None:
        exposure_weights = pd.Series(data=[1] * len(X.index))
        exposure_weights.index = y.index
    elif len(exposure_weights) != len(X.index):
        raise ValueError("sample_weights is not the same length as X")

    # Check length of data
    if len(X.index) <= 1:
        raise ValueError("Input data does not exist, please check the X dataset")

    # Subset rows dependent on where condition
    if where_condition is not None:
        use_idx = (X.query("&".join(where_condition))).index
    else:
        use_idx = X.index.values

    # # Remove rows with missing target value
    # predictor_table = predictor_table[~predictor_table['target'].isnull()]

    # Subset to useful columns
    plot_data = pd.DataFrame(
        {
            "actual": y[use_idx],
            "expected": y_pred[use_idx],
            "weights": exposure_weights[use_idx],
        }
    )

    # Sort by predicted value and calculate cumulative gains
    plot_data.sort_values(by=["expected"], ascending=False, inplace=True)
    plot_data["weighted_actual"] = plot_data["actual"] * plot_data["weights"]
    plot_data["cumulative_actual"] = plot_data["weighted_actual"].cumsum()
    plot_data["model_gains"] = plot_data["cumulative_actual"] / sum(
        plot_data["weighted_actual"]
    )
    plot_data["model_gains_lag"] = plot_data["model_gains"].shift(1, fill_value=0)
    plot_data["cumulative_population"] = plot_data["weights"].cumsum() / sum(
        plot_data["weights"]
    )
    plot_data.reset_index(inplace=True)  # Necessary for when tables are merged

    # Sort by target value and calculate cumulative gains
    plot_data_targ = pd.DataFrame(
        {
            "actual": y[use_idx],
            "expected": y_pred[use_idx],
            "weights": exposure_weights[use_idx],
        }
    )

    plot_data_targ = plot_data_targ.sort_values(by=["actual"], ascending=False)
    plot_data_targ["weighted_expected"] = (
        plot_data_targ["actual"] * plot_data_targ["weights"]
    )
    plot_data_targ["cumulative_expected"] = plot_data_targ["weighted_expected"].cumsum()
    plot_data_targ["maximum_gains"] = plot_data_targ["cumulative_expected"] / sum(
        plot_data_targ["weighted_expected"]
    )
    plot_data_targ["maximum_gains_lag"] = plot_data_targ["maximum_gains"].shift(
        1, fill_value=0
    )
    plot_data_targ["cumulative_population"] = plot_data_targ["weights"].cumsum() / sum(
        plot_data_targ["weights"]
    )
    # Necessary for when tables are merge
    plot_data_targ.reset_index(inplace=True)

    # Define additional columns for plotting
    plot_data.rename(
        columns={
            "weights": "model_weight",
            "cumulative_population": "cumulative_population_model_gains",
        },
        inplace=True,
    )
    plot_data["max_weight"] = plot_data_targ["weights"]
    plot_data["cumulative_population_max_gains"] = plot_data_targ[
        "cumulative_population"
    ]
    plot_data["maximum_gains"] = plot_data_targ["maximum_gains"]
    plot_data["maximum_gains_lag"] = plot_data_targ["maximum_gains_lag"]
    plot_data["random_gains_model_weight"] = plot_data[
        "cumulative_population_model_gains"
    ]
    plot_data["random_gains_model_weight_lag"] = plot_data[
        "random_gains_model_weight"
    ].shift(1, fill_value=0)
    plot_data["random_gains_max_weight"] = plot_data["cumulative_population_max_gains"]
    plot_data["random_gains_max_weight_lag"] = plot_data[
        "random_gains_max_weight"
    ].shift(1, fill_value=0)

    # Calculate gains ratio
    numerator = sum(
        (
            (plot_data["model_gains"] + plot_data["model_gains_lag"]) / 2
            - (
                plot_data["random_gains_model_weight"]
                + plot_data["random_gains_model_weight_lag"]
            )
            / 2
        )
        * plot_data["model_weight"]
    ) / sum(plot_data["model_weight"])
    denominator = sum(
        (
            (plot_data["maximum_gains"] + plot_data["maximum_gains_lag"]) / 2
            - (
                plot_data["random_gains_max_weight"]
                + plot_data["random_gains_max_weight_lag"]
            )
            / 2
        )
        * plot_data["max_weight"]
    ) / sum(plot_data["max_weight"])
    gains_ratio = numerator / denominator

    # Plot points
    fig, ax = plt.subplots()
    ax.plot(
        plot_data["cumulative_population_max_gains"],
        plot_data["maximum_gains"],
        "b-",
        label="Maximum Gains",
    )
    ax.plot(
        plot_data["cumulative_population_model_gains"],
        plot_data["model_gains"],
        "r-",
        label="Model Gains",
    )
    ax.plot(
        plot_data["cumulative_population_model_gains"],
        plot_data["random_gains_model_weight"],
        label="Random Gains",
    )

    # Plot text
    fig.legend(bbox_to_anchor=(0.5, -0.04), loc="lower center", ncol=3, frameon=False)
    figtext = (
        "Actual: {actual} | Current predicted: {pred} | No. of obs: {obs} | Where: {where}\n"
        "Date: {date} | Time: {time} \n"
        "Weight: {weight} | Model gains: {model_gains} | Theoretical Max Gains {max_gains} | Gains ratio {ratio}".format(
            actual=y.name,
            pred=y_pred.name,
            obs=len(X.index),
            where=where_condition,
            weight=exposure_weights.name,
            date=date.today(),
            time=datetime.now().strftime("%H:%M:%S"),
            model_gains=round(numerator, 4),
            max_gains=round(denominator, 4),
            ratio=round(gains_ratio, 4),
        )
    )
    plt.figtext(0.5, -0.1, figtext, ha="center")

    # Plot axes
    plt.xlim([0, 1])
    plt.xlabel("Cumulative Proportion of Population")
    plt.ylabel("Gains")
    plt.title("Model Gains")
    plt.tight_layout(pad=1.4)

    # Show plot
    # plt.show()

    return gains_ratio
