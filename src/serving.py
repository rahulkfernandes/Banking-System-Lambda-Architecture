import os
from typing import Dict
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame

load_dotenv(override=True)


def load_config() -> Dict:
    """Load configuration from environment variables"""
    return {
        "client_addr": os.getenv("CLIENT_ADDR"),
        "database_name": os.getenv("DATABASE_NAME"),
        "mysql_driver": os.getenv("MYSQL_DRIVER"),
        "mysql_url": os.getenv("MYSQL_URL"),
        "mysql_user": os.getenv("MYSQL_USER"),
        "mysql_password": os.getenv("MYSQL_PASSWORD"),
        "output_path": os.getenv("OUTPUT_PATH"),
    }


def create_spark_session(app_name: str = "CreditCardSystem") -> SparkSession:
    """
    Create and configure Spark session with MongoDB and MySQL connectors
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars", os.getenv("MYSQL_CONNECTOR_PATH"))
        .config("spark.checkpoint.dir", "/tmp/checkpoint/dir")
        .getOrCreate()
    )


def load_mysql_table(spark, config, table_name: str) -> DataFrame:
    """
    Load data from MySQL database.

    :param jdbc_url: JDBC URL for the MySQL database
    :param db_table: Name of the table to load data from
    :param db_user: Database username
    :param db_password: Database password
    :return: DataFrame containing the loaded MySQL data
    """
    return (
        spark.read.format("jdbc")
        .option("url", config["mysql_url"])
        .option("driver", config["mysql_driver"])
        .option("dbtable", table_name)
        .option("user", config["mysql_user"])
        .option("password", config["mysql_password"])
        .load()
    )


if __name__ == "__main__":

    config = load_config()
    spark = create_spark_session()

    transactions_table = load_mysql_table(spark, config, "transactions")
    cards_table = load_mysql_table(spark, config, "cards")
    customers_table = load_mysql_table(spark, config, "customers")
    card_types_table = load_mysql_table(spark, config, "credit_card_types")

    print("Transactions Table:")
    transactions_table.show()
    print("Cards Table:")
    cards_table.show()
    print("Customers Table:")
    customers_table.show()
    print("Credit Card Types Table:")
    card_types_table.show()
