"""End-to-end tests against the sample CSV."""
import pytest
from pyspark.sql import SparkSession

from src.core import PipelineConfig, PipelineEngine


class TestPipeline:

    def test_pipeline_produces_output(self, spark: SparkSession):
        config = PipelineConfig.from_yaml("config/pipeline.yaml")
        engine = PipelineEngine(spark, config)

        result = engine.run()

        assert result is not None
        assert result.count() > 0

    def test_output_has_expected_columns(self, spark: SparkSession):
        config = PipelineConfig.from_yaml("config/pipeline.yaml")
        engine = PipelineEngine(spark, config)

        result = engine.run()

        assert "instrument_id" in result.columns
        assert "vwap" in result.columns
        assert result.columns == config.output_columns

    def test_no_duplicate_instrument_date_pairs(self, spark: SparkSession):
        config = PipelineConfig.from_yaml("config/pipeline.yaml")
        engine = PipelineEngine(spark, config)

        result = engine.run()

        total = result.count()
        distinct = result.select("instrument_id", "trade_date").distinct().count()
        assert total == distinct

    def test_validation_rules_and_data_cleaning(self, spark: SparkSession):
        # checks the known bad rows in instruments.csv are handled
        config = PipelineConfig.from_yaml("config/pipeline.yaml")
        engine = PipelineEngine(spark, config)

        rows = {
            (r["instrument_id"], r["trade_date"]): r
            for r in engine.run().collect()
        }

        # AMZN only has zero-volume trades, so it should be gone
        assert ("AMZN", "2024-01-15") not in rows

        # NFLX uses 01/15/2024
        assert ("NFLX", "2024-01-15") in rows
        assert rows[("NFLX", "2024-01-15")]["vwap"] == pytest.approx(480.0)

        # ' AAPL' (5000 @ 185.00) should be merged into AAPL after trimming
        aapl_row = rows[("AAPL", "2024-01-15")]
        assert aapl_row["total_volume"] == pytest.approx(38000.0)
        expected_notional = 185.50 * 10000 + 186.00 * 15000 + 185.75 * 8000 + 185.00 * 5000
        assert aapl_row["total_notional"] == pytest.approx(expected_notional)
        assert aapl_row["vwap"] == pytest.approx(expected_notional / 38000.0)

        # MSFT: the -500 volume and 0.00 price rows should be dropped
        msft_row = rows[("MSFT", "2024-01-15")]
        assert msft_row["total_volume"] == pytest.approx(20000.0)

        # the two GOOGL EUR rows end up in non_usd_df
        assert engine.non_usd_df is not None
        assert engine.non_usd_df.count() == 2
