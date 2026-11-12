"""Transform steps used by the pipeline."""
from abc import ABC, abstractmethod
from typing import Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .functions import FUNCTIONS


class Transform(ABC):
    @abstractmethod
    def apply(self, df: DataFrame) -> DataFrame:
        ...


class FilterTransform(Transform):
    """Keep rows where <column> <operator> <value> holds."""

    OPERATORS = {
        "gt": lambda c, v: c > v,
        "gte": lambda c, v: c >= v,
        "lt": lambda c, v: c < v,
        "lte": lambda c, v: c <= v,
        "eq": lambda c, v: c == v,
        "ne": lambda c, v: c != v,
    }

    def __init__(self, column: str, operator: str, value: Any):
        self.column = column
        self.operator = operator
        self.value = value

    def apply(self, df: DataFrame) -> DataFrame:
        op_fn = self.OPERATORS[self.operator]
        return df.filter(op_fn(F.col(self.column), self.value))


class ExpressionTransform(Transform):
    """Adds a column computed by one of the functions in FUNCTIONS."""

    def __init__(self, output_column: str, function: str, inputs: list[dict[str, str]] | None = None):
        self.output_column = output_column
        self.function = function
        self.inputs = inputs or []

    def apply(self, df: DataFrame) -> DataFrame:
        cols = [F.col(inp["column"]) for inp in self.inputs]
        expr = FUNCTIONS[self.function](cols)
        return df.withColumn(self.output_column, expr)


class AggregateTransform(Transform):
    AGG_FUNCS = {
        "sum": F.sum,
        "avg": F.avg,
        "min": F.min,
        "max": F.max,
        "count": F.count,
    }

    def __init__(self, group_by: list[str], aggregations: list[dict[str, str]]):
        self.group_by = group_by
        self.aggregations = aggregations

    def apply(self, df: DataFrame) -> DataFrame:
        agg_exprs = [
            self.AGG_FUNCS[a["function"]](a["column"]).alias(a["alias"])
            for a in self.aggregations
        ]
        return df.groupBy(*self.group_by).agg(*agg_exprs)


def create_transform(step: dict[str, Any]) -> Transform:
    """Build a transform from one entry in the config's transformations list."""
    step_type = step.get("type")
    if step_type == "filter":
        cond = step["condition"]
        return FilterTransform(cond["column"], cond["operator"], cond["value"])
    if step_type == "expression":
        expr = step["expression"]
        return ExpressionTransform(step["output_column"], expr["function"], expr.get("inputs", []))
    if step_type == "aggregate":
        return AggregateTransform(step["group_by"], step["aggregations"])
    raise ValueError(f"Unknown transformation type: '{step_type}'")
