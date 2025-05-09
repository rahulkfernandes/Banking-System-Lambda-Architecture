import os
import time
import json
from decimal import Decimal
from datetime import datetime
from typing import Dict, List
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pyspark.sql.functions import date_format
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
        "kafka_topic": os.getenv("KAFKA_TOPIC"),
    }


def create_producer() -> KafkaProducer:
    """Create and return a Kafka producer instance."""
    return KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )


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


class Producer:
    __config = None

    def __init__(self, spark, kafka_producer, config):
        self.spark = spark
        self.kafka_producer = kafka_producer
        self.__config = config

        self.connection_properties = {
            "user": self.__config["mysql_user"],
            "password": self.__config["mysql_password"],
            "driver": self.__config["mysql_driver"],
        }

    def _conv_to_dict(self, row) -> Dict:
        transaction_dict = row.asDict()
        for key, value in transaction_dict.items():
            if isinstance(value, Decimal):
                transaction_dict[key] = float(value)
            elif isinstance(value, datetime):
                transaction_dict[key] = value.isoformat()
        return transaction_dict

    def _load_mysql_table(self, table_name: str) -> DataFrame:
        """
        Load data from MySQL database.

        :param jdbc_url: JDBC URL for the MySQL database
        :param db_table: Name of the table to load data from
        :param db_user: Database username
        :param db_password: Database password
        :return: DataFrame containing the loaded MySQL data
        """
        return (
            self.spark.read.format("jdbc")
            .option("url", self.__config["mysql_url"])
            .option("driver", self.__config["mysql_driver"])
            .option("dbtable", table_name)
            .option("user", self.__config["mysql_user"])
            .option("password", self.__config["mysql_password"])
            .load()
        )

    def _get_dictinct_dates(self):
        """Fetch all distinct dates from the transactions table using PySpark"""
        # Read transactions table from MySQL
        transactions_df = self.spark.read.jdbc(
            url=self.__config["mysql_url"],
            table="transactions",
            properties=self.connection_properties,
        )

        # Select distinct dates from timestamp column
        distinct_dates_df = (
            transactions_df.select(
                date_format(transactions_df.timestamp, "yyyy-MM-dd").alias("date")
            )
            .distinct()
            .orderBy("date")
        )

        # Collect dates as a list of strings
        dates = [row["date"] for row in distinct_dates_df.collect()]
        return dates

    def _get_transaction_by_day(self, date_str: str) -> List[Dict]:
        # Read transactions for the specific date
        query = f"SELECT * FROM transactions WHERE DATE(timestamp) = '{date_str}'"
        transactions_df = self.spark.read.jdbc(
            url=self.__config["mysql_url"],
            table=f"({query}) as temp",
            properties=self.connection_properties,
        )

        # Convert DataFrame to list of dictionaries
        transactions = [self._conv_to_dict(row) for row in transactions_df.collect()]
        return transactions

    def _send_day(self, date_str: str):
        """Send all transactions for a specific date to Kafka"""

        transactions = self._get_transaction_by_day(date_str)

        # Send each transaction to Kafka
        for transaction in transactions:
            try:
                self.kafka_producer.send(
                    self.__config["kafka_topic"], value=transaction
                )
            except KafkaError as e:
                print(
                    f"Failed to send transaction {transaction['transaction_id']}: {e}"
                )

        # Flush to ensure all messages are sent
        self.kafka_producer.flush()
        print(f"Sent {len(transactions)} transactions for {date_str}")

    def producer_process(self):
        """Main process for the producer"""
        dates = self._get_dictinct_dates()
        print(f"Found transactions for the following dates: {dates}")

        for date in dates:
            self._send_day(date)
            print(f"Finished sending transactions for {date}")
            # time.sleep(2)  # Pause before moving to the next day


def main():
    config = load_config()
    spark = create_spark_session("TransactionProducer")
    kafka_producer = create_producer()

    producer = Producer(spark, kafka_producer, config)
    producer.producer_process()

    print("All days' transactions have been sent.")
    kafka_producer.flush()
    kafka_producer.close()

    spark.stop()


if __name__ == "__main__":
    main()
