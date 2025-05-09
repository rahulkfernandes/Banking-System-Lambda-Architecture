import os
import glob
import shutil
from typing import Dict
from dotenv import load_dotenv
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    udf,
    lit,
    last,
    when,
    coalesce,
    sum as spark_sum,
    round as spark_round,
)

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


class BatchProcessor:
    __config = None

    def __init__(self, spark: SparkSession, config: Dict):
        self.spark = spark
        self.__config = config
        self.cards_table = None
        self.customers_table = None
        self.card_types_table = None
        self.transactions_table = None

    def _load_csv(self, path: str):
        """
        Load data from a CSV file.

        :param path: Path to the CSV file
        :return: DataFrame containing the loaded CSV data
        """
        return (
            self.spark.read.option("header", True)
            .option("inferSchema", True)
            .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss,SSS")
            .csv(path)
        )

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

    def _approve_transactions(self):
        # Approve all pending transactions
        self.transactions_table = self.transactions_table.withColumn(
            "status",
            when(col("status") == "pending", "approved").otherwise(col("status")),
        )

    def _verify_update_balance(self):
        # Get the last pending_balance for each card_id, ordered by timestamp
        final_pending = (
            self.transactions_table.orderBy("timestamp")
            .groupBy("card_id")
            .agg(last("pending_balance").alias("pending_balance"))
        )

        # Verify by recalculating pending_balance from approved transactions
        recalculated_pending = (
            self.transactions_table.filter(col("status") == "approved")
            .groupBy("card_id")
            .agg(spark_sum("amount").alias("recalculated_pending"))
            .withColumn(
                "recalculated_pending", spark_round(col("recalculated_pending"), 2)
            )
        )

        # Compare, including current_balance from self.cards_table
        comparison = (
            final_pending.join(
                self.cards_table.select("card_id", "current_balance"), "card_id", "left"
            )
            .withColumn("expected_sum", col("pending_balance") - col("current_balance"))
            .withColumn("expected_sum", spark_round(col("expected_sum"), 2))
            .join(recalculated_pending, "card_id", "left")
            .withColumn(
                "discrepancy",
                coalesce(col("expected_sum"), lit(0.0))
                - coalesce(col("recalculated_pending"), lit(0.0)),
            )
        )

        ## To verify pending balance calculations ##
        discrepancies = comparison.filter(col("discrepancy") != 0)
        if discrepancies.count() > 0:
            print(
                "Discrepancies found between stream and recalculated pending balances:"
            )
            discrepancies.show(truncate=False)

        # Update cards_table using the last pending_balance
        self.cards_table = (
            self.cards_table.join(final_pending, "card_id", "left")
            .withColumn(
                "current_balance",
                coalesce(col("pending_balance"), col("current_balance")),
            )
            .drop("pending_balance")
        )

    @staticmethod
    def _calculate_credit_score_adjustment(usage_percentage: int) -> int:
        """
        Calculate credit score adjustment based on credit usage percentage.

        Args:
            usage_percentage: Credit usage as a percentage of total available credit (0-100)

        Returns:
            int: Credit score adjustment (positive or negative)
        """
        # Credit utilization best practices suggest keeping usage below 30%
        if usage_percentage <= 10:
            # Excellent utilization: significant score improvement
            return 15
        elif usage_percentage <= 20:
            # Very good utilization
            return 10
        elif usage_percentage <= 30:
            # Good utilization
            return 5
        elif usage_percentage <= 50:
            # Fair utilization: small penalty
            return -5
        elif usage_percentage <= 70:
            # High utilization: moderate penalty
            return -15
        else:
            # Very high utilization: significant penalty
            return -25

    def _adjust_credit_score(self):
        adjustment_udf = udf(self._calculate_credit_score_adjustment, IntegerType())

        # Calculate total balance and limit per customer
        customer_usage = (
            self.cards_table.groupBy("customer_id")
            .agg(
                spark_sum("current_balance").alias("total_balance"),
                spark_sum("credit_limit").alias("total_limit"),
            )
            .withColumn(
                "usage_percentage", (col("total_balance") / col("total_limit")) * 100
            )
        )

        # Apply credit score adjustment
        customer_usage = customer_usage.withColumn(
            "adjustment", adjustment_udf(col("usage_percentage"))
        )

        # Update customers_table with adjusted credit_score
        self.customers_table = (
            self.customers_table.join(
                customer_usage.select("customer_id", "adjustment"),
                "customer_id",
                "left",
            )
            .withColumn("credit_score", col("credit_score") + col("adjustment"))
            .withColumnRenamed("adjustment", "credit_score_change")
        )

    @staticmethod
    def calculate_new_credit_limit(
        old_limit: float, credit_score_change: float
    ) -> float:
        """
        Calculate new credit limit based on credit score changes.

        Args:
            old_limit: Current credit limit
            credit_score_change: Amount the credit score changed

        Returns:
            float: New credit limit
        """
        # Only reduce limits when scores drop
        if credit_score_change >= 0:
            return old_limit

        # Calculate percentage reduction based on score drop
        if credit_score_change <= -20:
            # Significant drop: reduce by 15%
            reduction_factor = 0.85
        elif credit_score_change <= -10:
            # Moderate drop: reduce by 10%
            reduction_factor = 0.90
        else:
            # Small drop: reduce by 5%
            reduction_factor = 0.95

        return round(old_limit * reduction_factor, -2)  # Round to nearest 100

    def _new_credit_limit(self):
        # Define UDF for calculating new credit limit
        calculate_new_limit_udf = udf(self.calculate_new_credit_limit, DoubleType())

        # Join cards_table with customers_table to get credit_score_change
        cards_with_change = self.cards_table.join(
            self.customers_table.select("customer_id", "credit_score_change"),
            "customer_id",
            "left",
        )

        # Apply new credit limit UDF
        self.cards_table = cards_with_change.withColumn(
            "credit_limit",
            calculate_new_limit_udf(
                col("credit_limit"), coalesce(col("credit_score_change"), lit(0))
            ),
        ).drop("credit_score_change")

    def _reformat_tables(self):
        self.cards_table = self.cards_table.select(
            "card_id",
            "customer_id",
            "card_type_id",
            "card_number",
            "expiration_date",
            "credit_limit",
            "current_balance",
            "issue_date",
        ).orderBy("card_id")

        self.customers_table = self.customers_table.select(
            "customer_id",
            "name",
            "phone_number",
            "address",
            "email",
            "credit_score",
            "annual_income",
        ).orderBy("customer_id")

    def process_batch(self, streamed_path: str):
        self.transactions_table = self._load_csv(streamed_path)

        self.cards_table = self._load_mysql_table("cards")
        self.customers_table = self._load_mysql_table("customers")
        self.card_types_table = self._load_mysql_table("credit_card_types")

        self._approve_transactions()

        self._verify_update_balance()

        self._adjust_credit_score()

        self._new_credit_limit()

        self._reformat_tables()

        # Save all tables to csv
        self.save_to_csv(
            self.transactions_table,
            self.__config["output_path"],
            "batch_transactions.csv",
        )

        self.save_to_csv(
            self.cards_table, self.__config["output_path"], "cards_updated.csv"
        )

        self.save_to_csv(
            self.customers_table, self.__config["output_path"], "customers_updated.csv"
        )

        # self.save_to_csv(
        #     self.card_types_table,
        #     self.__config["output_path"],
        #     "credit_card_types.csv"
        # )

        self._write_mysql_table(self.customers_table, "customers")
        self._write_mysql_table(self.cards_table, "cards")
        self._write_mysql_table(self.transactions_table, "transactions")

        print("Cards Table:")
        self.cards_table.show()
        print("Customers Table:")
        self.customers_table.show()
        print("Credit Card Types Table:")
        self.card_types_table.show()
        print("Transactions Table:")
        self.transactions_table.show()

    def _write_mysql_table(self, df: DataFrame, table_name: str):
        """Function to write DataFrame back to MySQL table"""
        try:
            # Force materialization with explicit action
            cached_df = df.cache()
            cached_df.foreach(lambda _: None)  # Silent action to trigger execution

            # Write to MySQL
            (
                cached_df.write.format("jdbc")
                .option("url", self.__config["mysql_url"])
                .option("driver", self.__config["mysql_driver"])
                .option("dbtable", table_name)
                .option("user", self.__config["mysql_user"])
                .option("password", self.__config["mysql_password"])
                # .option("truncate", "true")
                .mode("overwrite")
                .save()
            )

            # print(f"Successfully wrote {cached_df.count()} rows to {table_name}")
            cached_df.unpersist()

        except Exception as e:
            print(f"Error writing to MySQL table {table_name}: {e}")
            raise

    def save_to_csv(self, df: DataFrame, output_path: str, filename: str) -> None:
        """
        Save DataFrame to a single CSV file.

        :param df: DataFrame to save
        :param output_path: Base directory path
        :param filename: Name of the CSV file
        """
        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)

        # Create full path for the output file
        full_path = os.path.join(output_path, filename)
        print(f"Saving to: {full_path}")  # Debugging output

        # Create a temporary directory in the correct output path
        temp_dir = os.path.join(output_path, "_temp")
        print(f"Temporary directory: {temp_dir}")  # Debugging output

        # Save to temporary directory
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

        # Find the generated part file
        csv_file = glob.glob(f"{temp_dir}/part-*.csv")[0]

        # Move and rename it to the desired output path
        shutil.move(csv_file, full_path)

        # Clean up - remove the temporary directory
        try:
            shutil.rmtree(temp_dir)
        except FileNotFoundError:
            print(f"Folder '{temp_dir}' does not exist.")


def main():
    """Main function batch process the entire data"""
    config = load_config()
    streamed_path = "data/results/stream_transactions.csv"

    # Initialize processor
    spark = create_spark_session()
    processor = BatchProcessor(spark, config)
    processor.process_batch(streamed_path)


if __name__ == "__main__":
    main()
