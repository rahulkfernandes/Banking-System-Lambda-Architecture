import csv
import mysql.connector
from typing import List
from mysql.connector import Error


class MySQLController:
    """Class to connect to mysql server"""

    def __init__(self, config: str):
        self.__config = config
        self.db_name = self.__config["database_name"]

        self.db_remote = self.connect_to_db_remote()
        self.db_cursor = self.db_remote.cursor()

    def connect_to_db_remote(self) -> mysql.connector.connection:
        try:
            if self.db_name is not None:
                db_remote = mysql.connector.connect(
                    host=self.__config["client_addr"],
                    user=self.__config["mysql_user"],
                    password=self.__config["mysql_password"],
                    database=self.db_name,
                )
                return db_remote
            else:
                print("Could not connect to MySQL Database.")
                return None
        except:
            print("Could not connect to MySQL server.")
            return None

    def get_all_transactions(self):
        self.db_cursor.execute("SELECT * FROM transactions")
        columns = [col[0] for col in self.db_cursor.description]
        rows = self.db_cursor.fetchall()
        return columns, rows

    def get_distinct_dates(self) -> List:
        """Fetch all distinct dates from the transactions table."""
        query = (
            "SELECT DISTINCT DATE(timestamp) FROM transactions ORDER BY DATE(timestamp)"
        )
        self.db_cursor.execute(query)
        dates = [
            row[0].isoformat() for row in self.db_cursor.fetchall()
        ]  # Convert date objects to strings
        return dates

    def get_transactions_by_date(self, date):
        """Fetch transactions for a specific date, ordered by timestamp."""
        query = (
            "SELECT * FROM transactions WHERE DATE(timestamp) = %s ORDER BY timestamp"
        )
        self.db_cursor.execute(query, (date,))
        columns = [col[0] for col in self.db_cursor.description]
        rows = self.db_cursor.fetchall()
        return columns, rows

    def get_cards_table(self):
        self.db_cursor.execute("SELECT * FROM cards")
        columns = [col[0] for col in self.db_cursor.description]
        rows = self.db_cursor.fetchall()
        return columns, rows

    def get_card_types_table(self):
        self.db_cursor.execute("SELECT * FROM credit_card_types")
        columns = [col[0] for col in self.db_cursor.description]
        rows = self.db_cursor.fetchall()
        return columns, rows

    def get_customers_table(self):
        self.db_cursor.execute("SELECT * FROM customers")
        columns = [col[0] for col in self.db_cursor.description]
        rows = self.db_cursor.fetchall()
        return columns, rows

    def close_root_conn(self):
        """Close root connection"""
        if self.root_remote:
            if self.root_cursor:
                self.root_cursor.close()
            self.root_remote.close()

    def close_db_conn(self):
        """Close database connection"""
        if self.db_remote:
            if self.db_cursor:
                self.db_cursor.close()
            self.db_remote.close()


class MySQLMigrator(MySQLController):
    """
    Class to connect to mysql server
    ONLY FOR TEST AUTOMATION
    """

    def __init__(self, config: str):
        self.__config = config
        self.db_name = self.__config["database_name"]
        # self.db_name = "test"

        self.root_remote = self.connect_to_root_remote()
        self.root_cursor = self.root_remote.cursor()

        self.initialize_db()

        self.db_remote = self.connect_to_db_remote()
        self.db_cursor = self.db_remote.cursor()

    def connect_to_root_remote(self) -> mysql.connector.connection:
        try:
            root_controller = mysql.connector.connect(
                host=self.__config["client_addr"],
                user=self.__config["mysql_user"],
                password=self.__config["mysql_password"],
            )
            if root_controller is None:
                print("Could not connect to MySQL server.")
            return root_controller

        except Exception as e:
            print("Could not connect to MySQL server.", e)
            return None

    def connect_to_db_remote(self) -> mysql.connector.connection:
        try:
            if self.db_name is not None:
                db_remote = mysql.connector.connect(
                    host=self.__config["client_addr"],
                    user=self.__config["mysql_user"],
                    password=self.__config["mysql_password"],
                    database=self.db_name,
                )
                return db_remote
            else:
                print("Could not connect to MySQL Database.")
                return None
        except:
            print("Could not connect to MySQL server.")
            return None

    def initialize_db(self):
        """DELETE database if exists and the creates a new database"""
        if self.root_remote and self.root_cursor:
            try:
                # Drop the database if it exists
                self.root_cursor.execute(f"DROP DATABASE IF EXISTS `{self.db_name}`")
                # Create the database
                self.root_cursor.execute(f"CREATE DATABASE `{self.db_name}`")
                self.root_remote.commit()
                print(f"Database '{self.db_name}' initialized successfully.")
            except mysql.connector.Error as err:
                print(f"Error creating/replacing database: {err}")
        else:
            print("Database connection not established.")

    def create_tables(self):
        try:
            if self.db_remote.is_connected():
                self.db_cursor.execute(
                    """CREATE TABLE IF NOT EXISTS customers (
                        customer_id   INT PRIMARY KEY,
                        name          TEXT,
                        phone_number  TEXT,
                        address       TEXT,
                        email         TEXT,
                        credit_score  INT,
                        annual_income DOUBLE
                    )"""
                )
                self.db_cursor.execute(
                    """CREATE TABLE IF NOT EXISTS credit_card_types (
                        card_type_id     INT PRIMARY KEY,
                        name             TEXT,
                        credit_score_min INT,
                        credit_score_max INT,
                        credit_limit_min INT,
                        credit_limit_max INT,
                        annual_fee       INT,
                        rewards_rate     DOUBLE
                    )"""
                )
                self.db_cursor.execute(
                    """CREATE TABLE IF NOT EXISTS cards (
                        card_id         INT PRIMARY KEY,
                        customer_id     INT,
                        card_type_id    INT,
                        card_number     TEXT,
                        expiration_date TEXT,
                        credit_limit    DOUBLE,
                        current_balance DOUBLE,
                        issue_date      DATE
                    )"""
                )
                self.db_cursor.execute(
                    """CREATE TABLE IF NOT EXISTS transactions (
                        transaction_id        INT PRIMARY KEY,
                        card_id               INT,
                        merchant_name         TEXT,
                        timestamp             DATETIME,
                        amount                DOUBLE,
                        location              TEXT,
                        transaction_type      TEXT,
                        related_transaction_id TEXT
                    )"""
                )
                self.db_remote.commit()
                print("Tables created successfully.")
        except Exception as e:
            print(f"Error while executing ddl statements: {e}")

    def import_csv_to_table(self, csv_path: str, table_name: str):
        """Import CSV file from a given path into a specified MySQL table."""
        try:
            with open(csv_path, "r") as csv_file:
                reader = csv.DictReader(csv_file)
                columns = reader.fieldnames  # Get column names from CSV header
                for row in reader:
                    values = tuple(row[col] for col in columns)
                    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
                    self.db_cursor.execute(query, values)
            self.db_remote.commit()
            print(
                f"Successfully imported data from {csv_path} into {table_name} table."
            )
        except FileNotFoundError:
            print(f"CSV file not found: {csv_path}")
        except Error as e:
            print(f"Error importing data into {table_name}: {e}")
            self.db_remote.rollback()
        except Exception as e:
            print(f"Error reading CSV file {csv_path}: {e}")
            self.db_remote.rollback()

    def import_customers(self, csv_path: str):
        """Import customers.csv into the customers table."""
        self.import_csv_to_table(csv_path, "customers")

    def import_cards(self, csv_path: str):
        """Import cards.csv into the cards table."""
        self.import_csv_to_table(csv_path, "cards")

    def import_transactions(self, csv_path: str):
        """Import transactions.csv into the transactions table."""
        self.import_csv_to_table(csv_path, "transactions")

    def import_credit_card_types(self, csv_path: str):
        """Import credit_card_types.csv into the credit_card_types table."""
        self.import_csv_to_table(csv_path, "credit_card_types")

    def get_transaction_count(self):
        """Get the number of transactions in the transactions table."""
        try:
            self.db_cursor.execute("SELECT COUNT(*) FROM transactions")
            count = self.db_cursor.fetchone()[0]
            return count
        except Error as e:
            print(f"Error retrieving transaction count: {e}")
            return None
