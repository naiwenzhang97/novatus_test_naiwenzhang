from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from .config import PipelineConfig
from src.transforms import create_transform

DATE_FORMATS = (
    "yyyy-MM-dd",
    "yyyy-MM-dd HH:mm:ss",
    "MM/dd/yyyy",
    "dd/MM/yyyy",
    "yyyy/MM/dd",
)


class PipelineEngine:
    """Loads the source, cleans it, then runs the configured steps in order."""

    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
        self.non_usd_df: DataFrame | None = None

    def run(self) -> DataFrame:
        df = self._load_source()
        df = self._clean_source(df)

        for step in self.config.transformations:
            df = create_transform(step).apply(df)

        df = self._apply_validations(df)

        if self.config.output_columns:
            df = df.select(*self.config.output_columns)

        return df

    def _load_source(self) -> DataFrame:
        path = self.config.source["path"]
        return self.spark.read.csv(path, header=True, inferSchema=True)

    def _clean_source(self, df: DataFrame) -> DataFrame:
        # strip stray whitespace
        for field in df.schema.fields:
            if isinstance(field.dataType, StringType):
                df = df.withColumn(field.name, F.trim(F.col(field.name)))

        # trade_date comes in different date formats, normalise to yyyy-MM-dd
        raw_date = F.col("trade_date").cast("string")
        # try_to_timestamp returns null instead of raising error
        parsed_date = F.coalesce(
            F.to_date(F.try_to_timestamp(raw_date)),
            *[F.to_date(F.try_to_timestamp(raw_date, F.lit(fmt))) for fmt in DATE_FORMATS],
        )
        df = df.withColumn("trade_date", F.date_format(parsed_date, "yyyy-MM-dd"))

        # drop rows with NULL date or volume <= 0 
        df = (
            df.withColumn("price", F.col("price").cast("double"))
            .withColumn("volume", F.col("volume").cast("double"))
            .filter(F.col("trade_date").isNotNull() & (F.col("volume") > 0))
        )

        # no FX rates available, so keep the main output USD only and
        # write the other currencies out separately instead of dropping them
        if "currency" in df.columns:
            self.non_usd_df = df.filter(F.col("currency") != "USD")
            self.non_usd_df.write.mode("overwrite").csv("data/output/non_usd", header=True)
            df = df.filter(F.col("currency") == "USD")

        return df

    def _apply_validations(self, df: DataFrame) -> DataFrame:
        for rule in self.config.validation_rules:
            col = F.col(rule["column"])
            if rule["check"] == "not_null":
                df = df.filter(col.isNotNull())
            elif rule["check"] == "positive":
                df = df.filter(col > 0)
        return df
