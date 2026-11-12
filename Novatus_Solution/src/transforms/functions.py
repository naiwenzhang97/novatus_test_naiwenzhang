"""Functions that expression steps can reference by name in the config."""
from pyspark.sql import Column
from pyspark.sql import functions as F


def multiply(cols: list[Column]) -> Column:
    return cols[0] * cols[1]


def divide(cols: list[Column]) -> Column:
    # null instead of failing when the denominator is 0
    return F.when(cols[1] != 0, cols[0] / cols[1]).otherwise(F.lit(None).cast("double"))


def current_timestamp(cols: list[Column]) -> Column:
    return F.current_timestamp()


FUNCTIONS = {
    "multiply": multiply,
    "divide": divide,
    "current_timestamp": current_timestamp,
}
