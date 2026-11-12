import pytest
from pyspark.sql import SparkSession

from src.transforms import (
    FilterTransform,
    ExpressionTransform,
    AggregateTransform,
    create_transform,
)


class TestTransforms:

    def test_filter_transform_gt(self, spark: SparkSession):
        df = spark.createDataFrame(
            [("A", 10.0), ("B", 0.0), ("C", -5.0), ("D", None)],
            ["id", "price"],
        )
        transform = FilterTransform(column="price", operator="gt", value=0)
        result = transform.apply(df).collect()

        assert len(result) == 1
        assert result[0]["id"] == "A"

    def test_expression_multiply_and_divide(self, spark: SparkSession):
        df = spark.createDataFrame(
            [("AAPL", 100.0, 20.0), ("AMZN", 150.0, 0.0)],
            ["instrument_id", "price", "volume"],
        )

        mult = ExpressionTransform(
            output_column="notional",
            function="multiply",
            inputs=[{"column": "price"}, {"column": "volume"}],
        )
        div = ExpressionTransform(
            output_column="vwap",
            function="divide",
            inputs=[{"column": "notional"}, {"column": "volume"}],
        )

        out = div.apply(mult.apply(df)).collect()
        rows = {r["instrument_id"]: r for r in out}

        assert rows["AAPL"]["notional"] == pytest.approx(2000.0)
        assert rows["AAPL"]["vwap"] == pytest.approx(100.0)
        # volume is 0 for AMZN -> null, not an error
        assert rows["AMZN"]["vwap"] is None

    def test_expression_current_timestamp(self, spark: SparkSession):
        df = spark.createDataFrame([("AAPL",)], ["instrument_id"])
        ts_transform = ExpressionTransform(
            output_column="processed_at",
            function="current_timestamp",
        )
        out = ts_transform.apply(df).collect()
        assert out[0]["processed_at"] is not None

    def test_aggregate_transform(self, spark: SparkSession):
        df = spark.createDataFrame(
            [
                ("AAPL", "2024-01-15", "USD", 1000.0, 100.0, 10.0),
                ("AAPL", "2024-01-15", "USD", 3000.0, 150.0, 20.0),
            ],
            ["instrument_id", "trade_date", "currency", "notional", "price", "volume"],
        )
        agg = AggregateTransform(
            group_by=["instrument_id", "trade_date", "currency"],
            aggregations=[
                {"column": "notional", "function": "sum", "alias": "total_notional"},
                {"column": "price", "function": "avg", "alias": "avg_price"},
                {"column": "volume", "function": "sum", "alias": "total_volume"},
            ],
        )
        res = agg.apply(df).collect()
        assert len(res) == 1
        row = res[0]
        assert row["currency"] == "USD"
        assert row["total_notional"] == pytest.approx(4000.0)
        assert row["avg_price"] == pytest.approx(125.0)
        assert row["total_volume"] == pytest.approx(30.0)

    def test_create_transform_factory_invalid_type(self):
        with pytest.raises(ValueError, match="Unknown transformation type"):
            create_transform({"type": "unsupported_type"})
