from datetime import date, datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def avse(
    X,
    y,
    y_pred,
    primary_predictor,
    exposure_weights=None,
    graph_groups=20,
    grouping="equal_dist",
    where_condition=None,
    return_data=False,
    unsuper=0,
):
    """
    Plots the actual and predicted values of a model against the values of a desired predictor

    Parameters:
    X (Pandas.DataFrame): Contains the model covariates (including exposure weights)
    y (Pandas.Series): Contains the actual target values
    y_pred (Pandas.Series): Contains the predicted target values
    primary_predictor (String): Specifies the name of the primary predictor to plot by
    exposure_weights (Pandas.Series): Contains the exposure weights. Default is even exposure if no exposure weights are provided.
    graph_groups (Int): For continous primary predictors specifies the number of groups to plot. Default: 20
    groupings (String): Desired method for creating the graph groups.
                equal_dist: bins equally spaced between min and max values of predictor. (Default)
                equal_size: bins of approximately equal exposure
                distinct: bin for each unique value of the primary predictor
    where_condition (list(String)): Where condition to apply to data in the form of ['col_name >= condition',
                                   'col_name >= condition']. Default: None
    return_data (Boolean): Specifies if the plotting data should be returned. Default is False.

    Returns:
    plot_data_agg( Pandas.DataFrame): Contains the plotting data and only returned if return_data is true.
    """

    # Check input types
    if not isinstance(X, pd.DataFrame):
        raise TypeError("Invalid type for X - should be pd.DataFram")
    if not isinstance(y, pd.Series):
        raise TypeError("Invalid type for y - should be pd.Series")
    if not isinstance(y_pred, pd.Series):
        raise TypeError("Invalid type for y_pred - should be pd.Series")
    if not (isinstance(exposure_weights, pd.Series) or exposure_weights is None):
        raise TypeError("Invalid type for exposure_weights - should be pd.Series")
    if type(graph_groups) is not int:
        raise TypeError("Inavalid type for graph_groups - should be int")
    if type(grouping) is not str:
        raise TypeError("Inavalid type for grouping - should be str")
    if not (isinstance(where_condition, list) or where_condition is None):
        raise TypeError("Inavalid type for where_condition - should be list of strings")
    if y.isna().sum() > 0:
        raise ValueError("The target contains missing values")

    # Define default bar width
    bar_width = 0.8

    # Apply where condition(s)
    if where_condition is not None:
        where_condition.append(primary_predictor + " == " + primary_predictor)

    else:
        where_condition = [primary_predictor + " == " + primary_predictor]

    use_idx = (X.query("&".join(where_condition), engine="python")).index.values

    null_where_condition = where_condition.copy()
    null_where_condition[:-1] = [
        cond
        for cond in null_where_condition[:-1]
        if (primary_predictor not in cond) or ("notnull" in cond)
    ]  # Removes where conditions applied primary predictor
    null_where_condition[-1] = primary_predictor + " != " + primary_predictor
    null_idx = (X.query("&".join(null_where_condition), engine="python")).index.values

    # If no exposure weights given, default to even exposure
    if exposure_weights is None:
        exposure_weights = pd.Series(
            data=[1] * len(X.index), index=X.index, name="exposure_weights"
        )

    # Bin primary predictor if continous
    if unsuper == 1:
        continous_flag = (
            False
            if (
                X.loc[use_idx, primary_predictor].dtype.name
                in ["category", "object", "bool"]
            )
            or "one_hot_encoder_" in primary_predictor
            or "mean_encoder_" in primary_predictor
            else True
        )
    else:
        continous_flag = (
            False
            if X[primary_predictor].dtype.name
            in ["category", "object", "bool", "boolean", "string"]
            or (len(X[primary_predictor].unique()) < 20)
            else True
        )

    # Bin continous variables
    if continous_flag and len(use_idx) > 0:
        if grouping not in ["equal_dist", "equal_size", "distinct"]:
            raise Exception("Invalid grouping method specified")

        if grouping == "equal_dist":

            # Get min/max data values
            data_min = X.loc[use_idx, primary_predictor].min()
            data_max = X.loc[use_idx, primary_predictor].max()

            # Define cuts
            cut_points = np.linspace(data_min, data_max, graph_groups + 1)

            cut_labels = (
                cut_points[0:graph_groups] + (cut_points[1] - cut_points[0]) / 2.0
            )
            bar_width = (cut_points[1] - cut_points[0]) * 0.8

            # Replace boundary cutpoints so new values outside current domain
            # will be captured
            cut_points[0] = -np.inf
            cut_points[-1] = np.inf

            # Defined binned version of primary predictor
            primary_predictor_grouped = pd.cut(
                X.loc[use_idx, primary_predictor], bins=cut_points, labels=cut_labels
            ).to_numpy()

        elif grouping == "equal_size":

            # Get distinct levels
            X["weights"] = exposure_weights
            levels = (
                X.loc[use_idx, [primary_predictor, "weights"]]
                .groupby(primary_predictor)
                .sum()
            )

            # Calculate cumulative weights
            levels["cumweight"] = levels["weights"].cumsum() / levels["weights"].sum()
            levels["lagcumweight"] = np.insert(
                levels[["cumweight"]].to_numpy()[0:-1], 0, 0.0
            )

            # Get target percentile cut points
            target_cuts = np.linspace(0, 1, graph_groups + 1)
            # add allowance for slightly above target cut as done in modelling
            # suite
            target_cuts[1:] += 1.5e-8

            # Apply cuts
            levels["label"] = (
                pd.cut(
                    levels["lagcumweight"],
                    bins=target_cuts,
                    labels=target_cuts[1:],
                    include_lowest=True,
                )
            ).to_numpy()
            levels[primary_predictor] = levels.index

            # Summarise levels
            aggregate_dictionary = {
                primary_predictor: [
                    min,
                    max,
                    lambda x: np.average(x, weights=levels.loc[x.index, "weights"]),
                ],
                "weights": [sum],
            }
            levels_summary = levels.groupby("label").agg(aggregate_dictionary)

            # Collapse low exposure bins to neighbouring bins
            iter = 0
            max_iter = 20
            while (levels_summary["weights"]["sum"].min() < 1 / graph_groups) and (
                iter < max_iter
            ):
                iter += 1

                # Add row number
                levels_summary["row"] = np.arange(levels_summary.shape[0])

                # Find rows with low exposures
                # if last row, group with prior, otherwise group with next
                if levels_summary.shape[0] > 1:
                    levels_summary.loc[
                        levels_summary.index[0] : levels_summary.index[-2], "row"
                    ] = np.where(
                        levels_summary["weights"]["sum"] < 1 / graph_groups,
                        levels_summary["row"] + 1,
                        levels_summary["row"],
                    )
                    row_idx = levels_summary.index[-1]
                    levels_summary.loc[row_idx, "row"] = (
                        levels_summary.loc[row_idx, "row"] - 1
                        if levels_summary.loc[row_idx, "row"] < 1 / graph_groups
                        else levels_summary[row_idx, "row"]
                    )

                    # Update level summary
                    levels_summary = levels_summary.groupby("row").agg(
                        aggregate_dictionary
                    )

                    # Break if too many iterations
                    if iter == max_iter:
                        raise Exception("Unable to converge to equal_size groupings")

                else:
                    # since there are no rows to collapse can exit loop
                    iter = max_iter

            # Define cut points
            cut_points = levels_summary[primary_predictor]["max"].to_numpy()
            cut_labels = levels_summary[primary_predictor]["<lambda_0>"].to_numpy()
            bar_width = min(cut_points[1:] - cut_points[:-1]) * 0.8

            # Replace boundary cutpoints so new values outside current domain
            # will be captured
            cut_points = np.insert(cut_points.astype(float), 0, -np.inf)
            cut_points[-1] = np.inf

            # Define binned version of primary predictor
            primary_predictor_grouped = pd.cut(
                X.loc[use_idx, primary_predictor], bins=cut_points, labels=cut_labels
            ).to_numpy()

        else:
            primary_predictor_grouped = X.loc[use_idx, primary_predictor].to_numpy()

    else:
        primary_predictor_grouped = X.loc[use_idx, primary_predictor].to_numpy()

    # Generate plot data
    x_lab = (
        primary_predictor
        if not continous_flag or grouping == "distinct"
        else primary_predictor + "_binned"
    )
    plot_data = pd.DataFrame(
        {
            "actual": y[use_idx],
            "expected": y_pred[use_idx],
            x_lab: primary_predictor_grouped,
            "weights": exposure_weights[use_idx],
        }
    )

    # Aggregate data for continous variables that have been binned
    plot_data_agg = pd.DataFrame()
    aggregate_dictionary = {
        "actual": [lambda x: np.average(x, weights=plot_data.loc[x.index, "weights"])],
        "expected": [
            lambda x: np.average(x, weights=plot_data.loc[x.index, "weights"])
        ],
        x_lab: lambda x: np.unique(x)[0],
        "weights": [sum],
    }

    plot_data_agg = plot_data.groupby(x_lab, as_index=False).agg(aggregate_dictionary)

    # Compute null primary predictor bin
    plot_data_nulls = None
    if len(null_idx) != 0:
        if len(plot_data_agg) > 0:
            x_pos = (
                plot_data_agg.loc[len(plot_data_agg) - 1, (x_lab, "<lambda>")]
                + 5 * (bar_width / 0.8)
                if continous_flag
                else "Missing"
            )
        else:
            x_pos = 0.0
        plot_data_nulls = [
            np.average(y[null_idx], weights=exposure_weights[null_idx]),
            np.average(y_pred[null_idx], weights=exposure_weights[null_idx]),
            x_pos,
            np.sum(exposure_weights.loc[null_idx]),
        ]
        plot_data_agg.loc[len(plot_data_agg)] = plot_data_nulls

    # Construct AvsE plot
    # fig, ax = plt.subplots()
    # # ax2 = ax.twinx()

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    ax2 = ax.twinx()

    # Plot line for continous variables
    if continous_flag:
        ax.plot(
            np.squeeze(plot_data_agg[x_lab].values),
            plot_data_agg["actual"],
            "b-",
            label="Actual",
        )
        ax.plot(
            np.squeeze(plot_data_agg[x_lab].values),
            plot_data_agg["expected"],
            "r-",
            label="Predicted",
        )
        ax2.bar(
            np.squeeze(plot_data_agg[x_lab].values),
            np.squeeze(plot_data_agg["weights"].values),
            label="Exposure",
            alpha=0.25,
            width=bar_width,
        )

    # Plot points for categorical variable
    else:
        ax.plot(
            np.squeeze(plot_data_agg[x_lab].values),
            plot_data_agg["actual"],
            "bo",
            label="Actual",
        )
        ax.plot(
            np.squeeze(plot_data_agg[x_lab].values),
            plot_data_agg["expected"],
            "ro",
            label="Predicted",
        )
        ax2.bar(
            np.squeeze(plot_data_agg[x_lab].values),
            np.squeeze(plot_data_agg["weights"].values),
            label="Exposure",
            alpha=0.25,
            width=bar_width,
        )

    # if plot_data_nulls is not None:
    #     print(plot_data_nulls)
    #     ax.plot(plot_data_nulls[2], plot_data_nulls[0], 'bo', label='Actual Null')
    #     ax.plot(plot_data_nulls[2], plot_data_nulls[1], 'ro', label='Expected Null')
    #     ax2.bar(plot_data_nulls[2], plot_data_nulls[3], alpha=0.25, width=bar_width, label = 'Exposure Null')

    # Plot text
    fig.legend(bbox_to_anchor=(0.5, -0.05), loc="lower center", ncol=3, frameon=False)
    figtext = (
        "Actual: {actual} | Current predicted: {pred} \n No. of obs: {obs} \n"
        "Date: {date} | Time: {time} \n Where: {where}".format(
            actual=y.name,
            pred=y_pred.name,
            obs=len(X.index),
            date=date.today(),
            time=datetime.now().strftime("%H:%M:%S"),
            where=where_condition[:-1],
        )
    )
    plt.figtext(0.5, -0.25, figtext, ha="center")

    # Plot labels and aesthetics
    ax.set_xlabel(primary_predictor)
    ax.set_ylabel("Actual / predicted target ")
    ax2.set_ylabel("Exposure")
    ax.set_title("AvsE for " + primary_predictor)
    plt.tight_layout(pad=1.4)

    # Change label of missing bin for continous variables
    if len(null_idx) > 0 and continous_flag:
        labels = ax.get_xticks().tolist()
        labels[-2] = plot_data_agg.loc[len(plot_data_agg) - 1, (x_lab, "<lambda>")]
        ax.set_xticks(labels[1:-1])
        ax2.set_xticks(labels[1:-1])
        labels[-2] = "Missing"
        ax.set_xticklabels(labels[1:-1])
        ax2.set_xticklabels(labels[1:-1])

    # Show plot
    ax.set_zorder(1)  # default zorder is 0 for ax1 and ax2
    ax.patch.set_visible(False)  # prevents ax1 from hiding ax2
    # prevents ax2 from hiding ax1 when saving as html
    ax2.patch.set_alpha(0.0)
    # plt.show()
    for tick in ax.get_xticklabels():
        tick.set_rotation(45)

    for tick in ax2.get_xticklabels():
        tick.set_rotation(45)

    if return_data:
        return (fig, plot_data_agg)
    else:
        return fig
