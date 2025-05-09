import os
import time
import json
import subprocess
from typing import Dict
from mysql_controller import MySQLMigrator
from dotenv import load_dotenv

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


def print_header():
    print("*" * 80)
    print("                        CREDIT PROCESSING SYSTEM")
    print("*" * 80, "\n")


def del_file(json_file_path: str):
    # Delete the file at the start if it exists
    if os.path.exists(json_file_path):
        os.remove(json_file_path)
        print(f"Deleted {json_file_path}")


def import_data():
    config = load_config()
    mysql_remote = MySQLMigrator(config)

    mysql_remote.create_tables()
    mysql_remote.import_customers("./data/dataset_27/customers.csv")
    mysql_remote.import_credit_card_types("./data/dataset_27/credit_card_types.csv")
    mysql_remote.import_cards("./data/dataset_27/cards.csv")
    mysql_remote.import_transactions("./data/dataset_27/transactions.csv")

    expected_transaction_count = mysql_remote.get_transaction_count()

    mysql_remote.close_root_conn()
    mysql_remote.close_db_conn()

    print("-" * 80, "\n")

    return expected_transaction_count


def main():
    print_header()

    # Import initial data into MySQL using data_import.py
    transaction_count = import_data()

    json_file_path = "src/transaction_counts.json"
    del_file(json_file_path)

    # # Start Kafka servers (ZooKeeper and Kafka)
    # kafka_home = os.path.expanduser(os.getenv("KAFKA_HOME"))
    # try:
    #     # Start ZooKeeper
    #     zookeeper_process = subprocess.Popen(
    #         [
    #             os.path.join(kafka_home, "bin/zookeeper-server-start.sh"),
    #             os.path.join(kafka_home, "config/zookeeper.properties")
    #         ],
    #         stdout=subprocess.DEVNULL,
    #         stderr=subprocess.DEVNULL
    #     )
    #     print("ZooKeeper started.")

    #     # Start Kafka server
    #     kafka_process = subprocess.Popen(
    #         [
    #             os.path.join(kafka_home, "bin/kafka-server-start.sh"),
    #             os.path.join(kafka_home, "config/server.properties")
    #         ],
    #         stdout=subprocess.DEVNULL,
    #         stderr=subprocess.DEVNULL
    #     )
    #     print("Kafka server started.")

    #     # Wait for Kafka servers to initialize
    #     time.sleep(10)
    # except Exception as e:
    #     print(f"Error starting Kafka servers: {e}")
    # return

    # Run stream processing (producer and consumer)
    try:
        # Start consumer in the background
        consumer_process = subprocess.Popen(["python", "src/consumer.py"])
        print("Consumer started.")

        # Start producer in the background
        producer_process = subprocess.Popen(["python", "src/producer.py"])
        print("Producer started.")

        # 4: Monitor transaction_counts.json for the consumer's count
        max_wait_time = 300  # Maximum wait time in seconds (5 minutes)
        start_time = time.time()

        while time.time() - start_time < max_wait_time:
            try:
                with open(json_file_path, "r") as f:
                    counts = json.load(f)
                    consumer_count = counts.get("consumer", 0)
                    print(
                        f"Current consumer count: {consumer_count}, Expected: {transaction_count}"
                    )
                    if consumer_count == transaction_count:
                        print(
                            f"Consumer processed {consumer_count} transactions. Starting batch processing..."
                        )
                        time.sleep(2)  # Arbitary wait time
                        break
                    elif consumer_count > transaction_count:
                        print(
                            "Consumer has consumed too many record! There is an Error"
                        )
                        exit()
            except (FileNotFoundError, json.JSONDecodeError):
                print("Waiting for transaction_counts.json to be created or valid...")
            time.sleep(5)  # Check every 5 seconds

        else:
            print("Timeout reached. Batch processing not triggered.")
            consumer_process.terminate()
            producer_process.terminate()
            return

        print("-" * 150, "\n")

        # Trigger batch processing
        try:
            subprocess.run(["python", "src/batch_processor.py"], check=True)
            print("Batch processing completed.")
        except subprocess.CalledProcessError as e:
            print(f"Error running batch processing: {e}")

    except Exception as e:
        print(f"Error during stream or batch processing: {e}")
        return

    finally:
        # Clean up by terminating producer and consumer
        consumer_process.terminate()
        producer_process.terminate()
        print("Producer and consumer terminated.")

        del_file(json_file_path)

        # # Terminate Kafka processes
        # print("Terminating Kafka processes...")

        # # Terminate ZooKeeper
        # try:
        #     zookeeper_process.terminate()  # Send SIGTERM
        #     zookeeper_process.wait(timeout=10)  # Wait up to 10 seconds
        #     print("ZooKeeper terminated.")
        # except subprocess.TimeoutExpired:
        #     print("ZooKeeper did not terminate in time. Killing...")
        #     zookeeper_process.kill()  # Send SIGKILL if needed

        # # Terminate Kafka server
        # try:
        #     kafka_process.terminate()  # Send SIGTERM
        #     kafka_process.wait(timeout=10)  # Wait up to 10 seconds
        #     print("Kafka server terminated.")
        # except subprocess.TimeoutExpired:
        #     print("Kafka server did not terminate in time. Killing...")
        #     kafka_process.kill()  # Send SIGKILL if needed


if __name__ == "__main__":
    main()
