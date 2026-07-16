from pyspark.sql import SparkSession


def build_spark_session(app_name: str, config: dict) -> SparkSession:
    spark_config = config.get("spark", {})
    master = spark_config.get("master", "local[*]")
    configs = spark_config.get("configs") or {}

    builder = SparkSession.builder.appName(app_name).master(master)
    for key, value in configs.items():
        builder = builder.config(key, str(value))

    return builder.getOrCreate()
