"""PySpark helpers: Python version alignment + safe cluster resource limits."""

import os


def apply_pyspark_python_configs(builder):
    """Spark workers (apache/spark image) use Python 3.10; driver must match."""
    py = os.environ.get("PYSPARK_PYTHON", "python3")
    driver_py = os.environ.get("PYSPARK_DRIVER_PYTHON", py)
    return (
        builder.config("spark.pyspark.python", py)
        .config("spark.pyspark.driver.python", driver_py)
        .config("spark.executorEnv.PYSPARK_PYTHON", py)
    )


def apply_cluster_resource_configs(builder, *, app_name: str = "lakehouse"):
    """
    Workers in compose are 2G RAM / 2 cores each.
    Default Spark would spawn 4x1G executors → OOM (exit 137) on workers.
    """
    instances = os.environ.get("SPARK_EXECUTOR_INSTANCES", "2")
    executor_mem = os.environ.get("SPARK_EXECUTOR_MEMORY", "1400m")
    driver_mem = os.environ.get("SPARK_DRIVER_MEMORY", "1536m")

    return (
        builder.config("spark.dynamicAllocation.enabled", "false")
        .config("spark.executor.instances", instances)
        .config("spark.executor.cores", "1")
        .config("spark.executor.memory", executor_mem)
        .config("spark.driver.memory", driver_mem)
        .config("spark.cores.max", str(int(instances) * 1))
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.ui.showConsoleProgress", "true")
    )
