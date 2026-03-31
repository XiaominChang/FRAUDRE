# Imports
from datetime import date, datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def partial_dependence_plot(
    X,
    primary_predictor,
    model_obj,
    predict_fun,
    exposure_weights=None,
    graph_groups=20,
    grouping="equal_dist",
    where_condition=None,
    sub_sample=1.0,
    return_data=False,
    preprocess_pipeline=None,
):
    """
    Plots the actual and predicted values of a model against the predicted
    value percentile bands
    Parameters:
    X (Pandas.DataFrame): Contains model data
    primary_predictor (String): Specifies the name of the primary predictor that the plot is dependent on.
    model_obj (model object): Model object that is used to score the model.
    predict_fun (function): Function used to get predictions from the model object.
    exposure_weights (Pandas.Series): Contains the exposure weights. Default is even exposure if no exposure weights are provided.
    graph_groups (Int): For continous primary predictors specifies the number of groups to plot. Default: 20
    groupings (String): Desired method for creating the graph groups.
                equal_dist: bins equally spaced between min and max values of predictor. (Default)
                equal_size: bins of approximately equal exposure
                distinct: bin for each unique value of the primary predictor
    where_condition (list(String)): Where condition to apply to data in the form of ['col_name >= condition',
                                   'col_name >= condition']. Default: None
    sub_sample (Float): Proportion of data to use when producing partial dependene plot (should be between (0,1]))
    return_data (Boolean): Specifies if the plotting data should be returned. Default is False.
    preprocess_pipeline (Pipeline): sklearn preprocessing pipeline if data needs to be imputed and encoded before model scoring.


    Returns:
        plot_data_agg (Pandas.DataFrame): Contains the plotting data and only returned if return_data is true.
    """

    # Check input types
    if not isinstance(X, pd.DataFrame):
        raise TypeError("Invalid type for X - should be pd.DataFrame")
    if not isinstance(primary_predictor, str):
        raise TypeError("Invalid type for primary_predictor - should be str")
    if not (isinstance(exposure_weights, pd.Series) or exposure_weights is None):
        raise TypeError("Invalid type for exposure_weights - should be pd.Series")
    if type(graph_groups) is not int:
        raise TypeError("Inavalid type for graph_groups - should be int")
    if type(grouping) is not str:
        raise TypeError("Inavalid type for grouping - should be str")
    if not (isinstance(where_condition, list) or where_condition is None):
        raise TypeError("Inavalid type for where_condition - should be list of strings")
    if not isinstance(sub_sample, float):
        raise TypeError("Inavalid type for sub_sample - should be a float")
    if sub_sample <= 0.0 or sub_sample > 1.0:
        raise ValueError("Invalid value for sub_sample, should be between (0,1]")

    # Apply where condition(s)
    if where_condition is not None:
        where_condition.append(primary_predictor + " == " + primary_predictor)

    else:
        where_condition = [primary_predictor + " == " + primary_predictor]

    use_idx = (X.query("&".join(where_condition), engine="python")).index.values

    # If no exposure weights given, default to even exposure
    if exposure_weights is None:
        exposure_weights = pd.Series(
            data=[1] * len(X.index), index=X.index, name="exposure_weights"
        )

    # Preform subsampling if applicable
    if sub_sample < 1.0:
        size = int(np.floor(len(use_idx) * sub_sample))
        np.random.seed(0)
        use_idx = np.random.choice(use_idx, size, replace=True)

    # Bin primary predictor if continous
    continous_flag = (
        False
        if (
            X.loc[use_idx, primary_predictor].dtype.name
            in ["category", "object", "bool", "boolean", "string"]
        )
        or (len(X[primary_predictor].unique()) < 20)
        else True
    )

    if continous_flag:

        # Define cuts
        quantiles = np.linspace(0.0, 1.0, graph_groups + 1)
        if graph_groups < 100:
            quantiles = np.insert(quantiles, 1, 0.01)
            quantiles = np.insert(quantiles, len(quantiles) - 1, 0.99)

        cut_points = (
            X.loc[use_idx, primary_predictor].quantile(quantiles)
        ).to_numpy()  # might need to update edge cut_points

        cut_labels = cut_points[0:graph_groups] + (cut_points[1] - cut_points[0]) / 2.0
        bar_width = (cut_points[1] - cut_points[0]) * 0.8

        # Defined binned version of primary predictor
        primary_predictor_groups = cut_labels

        # If only one unique level then define primary_predictor_groups as
        # smallest graph_group unique values (needed for some of the customer
        # count features)
        if len(pd.Series(primary_predictor_groups).unique()) == 1:
            primary_predictor_groups = X.loc[use_idx, primary_predictor].unique()
            primary_predictor_groups = np.sort(primary_predictor_groups)[
                0 : graph_groups + 1
            ]

    else:
        primary_predictor_groups = X.loc[use_idx, primary_predictor].unique()

    def calculate_score(
        level, df, primary_predictor, model_obj, predict_fun, preprocess_pipeline
    ):
        df_modified = df.copy(deep=True)

        # set primary predictor to desired level
        df_modified[primary_predictor] = level

        score = np.average(predict_fun(model_obj, preprocess_pipeline, df_modified))

        return score

    # Calculate average partial dependence scores
    scores = [
        calculate_score(
            level, X, primary_predictor, model_obj, predict_fun, preprocess_pipeline
        )
        for level in primary_predictor_groups
    ]

    plot_data = pd.DataFrame({"labels": primary_predictor_groups, "scores": scores})

    # Construct parital dependence plot
    fig, ax = plt.subplots()

    # Plot line for continous variable
    if continous_flag:
        lns1 = ax.plot(
            plot_data["labels"], plot_data["scores"], "b-", label="Partial dependence"
        )

    # Plot points for catgorical variable
    else:
        lns1 = ax.plot(
            plot_data["labels"], plot_data["scores"], "bo", label="Partial dependence"
        )

    # Plot text
    figtext = (
        "Primary Predictor: {primary_predictor} | No. of obs: {obs} \n"
        "Date: {date} | Time: {time} \n Where: {where} | Weight: {weight} ".format(
            primary_predictor=primary_predictor,
            obs=len(X.index),
            date=date.today(),
            time=datetime.now().strftime("%H:%M:%S"),
            where=where_condition,
            weight=exposure_weights.name,
        )
    )
    plt.figtext(0.5, -0.25, figtext, ha="center")

    # Plot axes
    ax.set_xlabel(primary_predictor)
    ax.set_ylabel("Partial dependence")
    ax.set_title("Partial Dependence Plot for " + primary_predictor)
    plt.tight_layout(pad=1.4)
    if not continous_flag:
        plt.xticks(rotation=90, fontsize=8)

    # Show plot
    ax.set_zorder(1)  # default zorder is 0 for ax1 and ax2
    ax.patch.set_visible(False)  # prevents ax1 from hiding ax2
    # plt.show()

    if return_data:
        return (fig, plot_data)
    else:
        return fig
