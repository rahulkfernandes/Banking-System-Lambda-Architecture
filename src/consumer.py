import os
import re
import csv
import json
from typing import Dict
from decimal import Decimal
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

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


def create_consumer(topic_name):
    """Create and return a Kafka consumer instance."""
    return KafkaConsumer(
        topic_name,
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="user-data-consumer-group",
        # consumer_timeout_ms=10000,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
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


class StreamProcessor:
    __config = None

    def __init__(self, spark: SparkSession, consumer: KafkaConsumer, config: Dict):
        self.spark = spark
        self.consumer = consumer
        self.__config = config
        self.cards_table = None
        self.customers_table = None
        self.card_types_table = None

        self.pending_balances = None

        self.zip_regex = re.compile(r",\s[A-Z]{2}\s(\d{5})$")

        self.get_mysql_data()
        self._initialize_csv()

    def _initialize_csv(self):
        """Initialize the CSV file with headers if it doesn't exist or is empty."""
        csv_file = os.path.join(self.__config["output_path"], "stream_transactions.csv")
        os.makedirs(self.__config["output_path"], exist_ok=True)

        self.csv_headers = [
            "transaction_id",
            "card_id",
            "merchant_name",
            "timestamp",
            "amount",
            "location",
            "transaction_type",
            "related_transaction_id",
            "status",
            "pending_balance",
        ]

        try:
            with open(csv_file, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=self.csv_headers)
                writer.writeheader()
            print(f"Initialized CSV at {csv_file}")
        except Exception as e:
            print(f"Error initializing CSV: {e}")

    def _append_to_csv(self, transaction: Dict):
        """Append a single transaction to the CSV file."""
        csv_file = os.path.join(self.__config["output_path"], "stream_transactions.csv")
        try:
            with open(csv_file, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=self.csv_headers)
                writer.writerow(transaction)
        except Exception as e:
            print(f"Error appending to CSV: {e}")

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

    def get_mysql_data(self):
        """Get MySQL table data and convert to dictionaries for lookup."""
        # Get cards table
        self.cards_table = self._load_mysql_table("cards")

        rows = self.cards_table.select("card_id", "current_balance").collect()
        # Build a nested dict
        # { card_id: { "current_balance": <value>, "pending_balance": <init> } }
        self.pending_balances = {
            row["card_id"]: {
                "current_balance": row["current_balance"],
                "pending_balance": None,
            }
            for row in rows
        }

        # Get customers table
        self.customers_table = self._load_mysql_table("customers")

        # Get credit_card_types table
        self.card_types_table = self._load_mysql_table("credit_card_types")

    def _extract_zip_code(self, address: str) -> str:
        """
        Extract the ZIP code using regex.
        """
        try:
            # Match 5-digit ZIP code at the end, preceded by state and space
            match = self.zip_regex.search(address.strip())
            return match.group(1) if match else ""
        except Exception as e:
            print("Error Extracting ZIP:", str(e))
            return ""

    def _is_location_close_enough(self, zip1: str, zip2: str) -> bool:
        """Determine if merchant location is close enough to the customer's address.

        Returns:
            bool: True if the locations are close enough to approve, False if too far apart
        """
        if not zip1 or not zip2 or len(zip1) < 5 or len(zip2) < 5:
            # Can't determine distance with invalid zips
            return False  # Safer to reject when we can't verify

        # Check first digits of zip codes to determine proximity
        # Increased allowed distance by allowing first digit to be different
        if zip1[:1] != zip2[:1]:
            # Different first digit - very far apart (different regions)
            return False

        if zip1[:2] != zip2[:2]:
            # Different second digit but same first digit
            # Moderately far but will now approve these
            return True

        # Same first 2+ digits - close enough
        return True

    def _process_transaction(self, transaction: Dict) -> Dict:
        """
        Process a transaction: validate, update MySQL, and log if declined.

        Args:
            transaction: Dictionary containing transaction details.

        Returns:
            Dictionary with transaction status and reason.
        """
        card_id = transaction["card_id"]
        transaction_id = transaction["transaction_id"]
        amount = transaction["amount"]
        transaction_type = transaction["transaction_type"]
        merchant_location = transaction["location"]

        # # Get card details
        # if card_id not in self.cards_dict:
        #     print(f"DECLINED: Transaction {transaction_id} - Card ID {card_id} not found")
        #     return {"status": "declined", "reason": f"Card ID {card_id} not found"}

        card_details = (
            self.cards_table.filter(col("card_id") == card_id).first().asDict()
        )
        credit_limit = card_details["credit_limit"]
        current_balance = card_details["current_balance"]
        pending_balance = self.pending_balances[card_id].get("pending_balance", None)

        customer_id = card_details["customer_id"]

        # # Get customer address
        # if customer_id not in self.customers_dict:
        #     print(f"DECLINED: Transaction {transaction_id} - Customer ID {customer_id} not found")
        #     return {"status": "declined", "reason": f"Customer ID {customer_id} not found"}

        customer_address = self.customers_table.filter(
            col("customer_id") == customer_id
        ).first()["address"]

        # Initialize pending_balance with current_balance if None (first transaction)
        if pending_balance is None:
            pending_balance = current_balance

        # Initialize approval and reason
        approval = True
        reason = "Approved"

        # Skip credit limit and distance validations for refunds/cancellations
        if amount >= 0 and transaction_type not in ["refund", "cancellation"]:
            # Validation 1: Decline if amount >= 50% of credit limit
            if amount >= 0.5 * credit_limit:
                approval = False
                reason = f"Amount {amount} exceeds 50% of credit limit {credit_limit}"

            # Validation 2: Decline if locations are too far
            if approval:
                merchant_zip = self._extract_zip_code(merchant_location)
                customer_zip = self._extract_zip_code(customer_address)
                if not self._is_location_close_enough(merchant_zip, customer_zip):
                    approval = False
                    reason = f"Merchant location {merchant_location} too far from customer address {customer_address}"

        # Validation 3: Decline if total balance exceeds credit limit
        total_balance = pending_balance + amount
        if total_balance > credit_limit:
            approval = False
            reason = (
                f"Total balance {total_balance} exceeds credit limit {credit_limit}"
            )

        # Determine status
        status = "pending" if approval else "declined"

        # Calculate new pending balance for the transaction output
        new_pending_balance = pending_balance
        if approval or transaction_type in ["refund", "cancellation"]:
            new_pending_balance = pending_balance + amount
            # Update in-memory cards_dict
            self.pending_balances[card_id]["pending_balance"] = new_pending_balance

        # Log declined transactions
        if not approval:
            print(f"DECLINED: Transaction {transaction_id} - {reason}")

        transaction["status"] = status
        transaction["pending_balance"] = round(new_pending_balance, 2)
        return transaction

    def consume(self):
        print(f"Starting to consume messages from topic '{self.consumer.topics}'...")
        print("Waiting for messages... (Press Ctrl+C to stop)")

        transaction_count = 0

        try:
            for message in self.consumer:
                # print("-" * 150)
                print(f"Received message at offset {message.offset}")
                transaction = message.value

                # Process message
                processed_transaction = self._process_transaction(transaction)
                # print(processed_transaction)
                # Append transaction to CSV
                self._append_to_csv(processed_transaction)

                # Increment counter and write to JSON file
                transaction_count += 1
                with open("src/transaction_counts.json", "w") as f:
                    json.dump({"consumer": transaction_count}, f)

                # print("")

        except KeyboardInterrupt:
            print("\nConsumer stopped by user")
        except Exception as e:
            print("Error While Consuming Stream:", str(e))
        finally:
            self.consumer.close()
            self.spark.stop()
            print("Consumer closed")


def main():
    """Main function to consume messages from Kafka topic."""

    config = load_config()
    spark = create_spark_session("TransactionConsumer")
    consumer = create_consumer(config["kafka_topic"])

    processor = StreamProcessor(spark, consumer, config)
    processor.consume()

    # spark.stop()


if __name__ == "__main__":
    main()
