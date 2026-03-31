# Imports
import base64
import io


def segment_data(X, predictor, seg_type):
    """
    Segment data for plotting

    Parameters
        X (Pandas.DataFrame): Contains the model covariates (including exposure weights)
        predictor (Str): Name of predictor to segment data by
        seg_type (Str): Specifies how the data will be segmented, one of all, no_missing, missing, 90_dist, percentiles

    Returns
        idx (np.array): Indices of the data to include

    """
    if seg_type == "all":
        idx = X.index.values
    elif seg_type == "no_missing":
        idx = (
            X.query("&".join([predictor + ".notnull()"]), engine="python")
        ).index.values
    elif seg_type == "missing":
        idx = (
            X.query("&".join([predictor + ".isnull()"]), engine="python")
        ).index.values
    elif seg_type == "90_dist":
        lq = X[predictor].quantile(q=0.05)
        uq = X[predictor].quantile(q=0.95)
        idx = (
            X.query(
                "&".join([predictor + ">" + str(lq), predictor + "<" + str(uq)]),
                engine="python",
            )
        ).index.values
    else:
        print("Invalid data segmentation type given, using all the data")
        idx = X.index.values

    return idx


def fig_to_base64(fig):
    """
    Encodes matplotlib figure

    Parameters
        fig (MatPlotLib figure): Figure to encode

    Returns
        Encoded figure
    """
    img = io.BytesIO()
    fig.savefig(img, format="png", bbox_inches="tight")
    img.seek(0)

    return base64.b64encode(img.getvalue())
