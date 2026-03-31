# Imports
import matplotlib.pyplot as plt
import numpy as np


def prec_rec_thresh(precisions, recalls, thresholds, target_precision=0.9):
    """
    functions for threshold calibration
    :param precisions: value of precisions
    :param recalls:  value of recalls
    :param thresholds: value of thresholds
    :param target_precision: value of target precision
    :return: plot
    """
    # Find the index where precision first reaches or exceeds the
    # target_precision
    idx = np.argmax(precisions >= target_precision)

    # If precision never reaches target_precision, use the maximum precision
    # instead
    if precisions[idx] < target_precision:
        target_precision = np.max(precisions)
        idx = np.argmax(precisions == target_precision)

    # Find corresponding recall and threshold values
    recall_90_precision = recalls[idx]
    threshold_90_precision = thresholds[idx - 1] if idx > 0 else thresholds[0]

    # Plot precision and recall against thresholds
    plt.plot(thresholds, precisions[:-1], "b--", label="Precision", linewidth=2)
    plt.plot(thresholds, recalls[:-1], "g-", label="Recall", linewidth=2)

    # Plot target precision line and markers
    plt.plot(
        [threshold_90_precision, threshold_90_precision], [0.0, target_precision], "r:"
    )
    plt.plot([0, threshold_90_precision], [target_precision, target_precision], "r:")
    plt.plot([threshold_90_precision], [target_precision], "ro")
    plt.plot([threshold_90_precision], [recall_90_precision], "ro")

    # Set axis limits and labels
    plt.axis([0, 1, 0, 1])
    plt.xlabel("Threshold")
    plt.xticks(np.arange(0, 1.05, 0.05))
    # Rotate Tick Labels
    plt.xticks(rotation=45)
    plt.yticks(np.arange(0, 1.1, 0.1))

    # Add grid and legend
    plt.grid(True)
    plt.legend()

    # # Show the plot
    # plt.show()
