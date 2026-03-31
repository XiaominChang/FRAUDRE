# Train-test split using hashing
# Hash methodology is FarmHash
# Hashing means that the train-test split should be reproducible across
# different data cuts (not random and requiring a seed)

from typing import Any, Callable

import farmhash
import pandas as pd

test_ratio = 0.3
buckets = 10

# define some custom types (optional)
HashFunc = Callable[[Any], int]
TestSetCheckFunc = Callable[[int], bool]


def generate_farmhash_fingerprint(value: Any) -> int:
    """Convert a value into a hashed value using farmhash"""
    return farmhash.fingerprint64(str(value))


def convert_hash_to_bucket(hashed_value: int, total_buckets: int) -> int:
    """Assign a bucket using modulo operator"""
    return hashed_value % total_buckets


def test_set_check(bucket: int) -> bool:
    """Check if the bucket should be included in the test set

    This is an arbirtary function, you could change this for your own
    requirements

    In this case, the datapoint is assigned to the test set if the bucket
    number is less than the test ratio x total buckets.
    """
    return bucket < test_ratio * buckets


def assign_hash_bucket(value: Any, hash_func: Callable) -> int:
    """Assign a bucket to an input value using hashing algorithm"""
    hashed_value = hash_func(value)
    bucket = convert_hash_to_bucket(hashed_value, total_buckets=buckets)
    return bucket


def hash_train_test_split(
    df: pd.DataFrame,
    split_col: str,
    approx_test_ratio: float,
    hash_func: HashFunc,
    test_set_check_func: TestSetCheckFunc,
) -> tuple:
    """Split the data into a training and test set based of a specific column

    This function adds an additional column to the dataframe. This is for
    demonstration purposes and is not required. The test set check could all
    be completed in memory by adapting the test_set_check_func

    Args:
        df (pd.DataFrame): original dataset
        split_col: name of the column to use for hashing which uniquely
            identifies a datapoint
        approx_test_ratio: float between 0-1. This is an approximate ratio as
            the hashing algo will not necessarily provide a uniform bucket
            distribution for small datasets
        hash_func: hash function to use to encode the data
        test_set_check_func: function used to check if the bucket should be
            included in the test set
    Returns:
        tuple: Two dataframes, the first is the training set and the second
            is the test set
    """

    # assign bucket
    df["bucket"] = df[split_col].apply(assign_hash_bucket, hash_func=hash_func)

    # generate 'mask' of boolean values which define the train/test split
    in_test_set = df["bucket"].apply(test_set_check_func)

    df["set"] = "Train"
    df.loc[in_test_set, "set"] = "Test"

    df = df.drop("bucket", axis=1)

    return df


def hash_split_data(result_df: pd.DataFrame, split_column: str):
    return hash_train_test_split(
        result_df,
        split_col=split_column,
        approx_test_ratio=test_ratio,
        hash_func=generate_farmhash_fingerprint,
        test_set_check_func=test_set_check,
    )
