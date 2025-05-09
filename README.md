# Project-4

## Banking System with Lambda Architecture

## Description
This project implements a credit card transaction processing system using the Lambda Architecture with stream and batch processing components, as well as a serving layer to query the data. The system tracks card balances, approves or declines transactions based on validation rules, and manages data flow between stream and batch layers.

1. **Stream Layer**: Processes transactions in real-time, validating them and tracking pending balances.
2. **Batch Layer**: Periodically processes accumulated data, updating the master dataset with finalized values.
3. **Serving Layer**: Provides access to the processed data through relational database.

## Dataset
### 1. cards.csv
Contains details about issued credit cards.
Columns:
- card_id (Primary Key): Unique identifier for the card.
- customer_id (Foreign Key): Links to the customer who owns the card.
- card_type_id (Foreign Key): Links to the card type (e.g., Basic, Gold).
- card_number: Masked credit card number.
- expiration_date: Card expiry date (MM/YY).
- credit_limit: Maximum allowable credit.
- current_balance: Outstanding balance on the card.
- issue_date: Date the card was issued.

### 2. credit_card_types.csv
Defines credit card tiers and their eligibility criteria.
Columns:
- card_type_id (Primary Key): Unique identifier for the card type.
- name: Card tier name (e.g., Platinum, Diamond).
- credit_score_min/credit_score_max: Required credit score range.
- credit_limit_min/credit_limit_max: Credit limit bounds for the tier.
- annual_fee: Yearly fee for the card.
- rewards_rate: Cashback/rewards percentage.

### 3. customers.csv
Stores customer demographic and financial data.
Columns:
- customer_id (Primary Key): Unique identifier for the customer.
- name: Customer’s full name.
- phone_number: Contact number.
- address: Physical address.
- email: Email address.
- credit_score: Numeric credit score (580–850).
- annual_income: Yearly income in USD.

### 4. transactions.csv
Logs all credit card transactions.
Columns:
- transaction_id (Primary Key): Unique transaction identifier.
- card_id (Foreign Key): Links to the card used.
- merchant_name: Name of the merchant.
- timestamp: Date and time of the transaction.
- amount: Transaction amount (negative for refunds).
- location: Merchant location (address, city, state, ZIP).
- transaction_type: Type of transaction (e.g., purchase, refund, wire_transfer).
- related_transaction_id: Links to refunds/cancellations (if applicable).

## Prerequisites
- Python 3.12
- OpenJDK 17
- Java 8+ (required for Kafka)
- MySQL 9.0.1

## Installation

### Clone Git Repository
```
git clone git@git.rc.rit.edu:s25-dsci-644-rahul/project-4.git
```

### Download and Setup Kafka
```bash
# Create a directory for Kafka
mkdir -p ~/kafka
cd ~/kafka

# Download Kafka 3.8.1
wget https://downloads.apache.org/kafka/3.8.1/kafka_2.13-3.8.1.tgz

# Extract the archive
tar -xzf kafka_2.13-3.8.1.tgz

# [Optional] Set up environment variables (add these to your ~/.bashrc or ~/.zshrc for permanence)
export KAFKA_HOME=~/kafka/kafka_2.13-3.8.1
export PATH=$PATH:$KAFKA_HOME/bin
```
### Install Dependencies

```bash
# Create and activate a virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate  # On Windows, use: venv\Scripts\activate

# Install the required packages
pip install -r requirements.txt
```
### Setting Up Environment Variables
1. Create your local environment file:
```bash
cp .env.example .env
```
2. Update the .env file with your database credentials and paths

### Required Connectors
- Download MySQL Connector/J (9.1.0) and save the jar file in an appropriate directory and add the path do your .env file

## Usage
Ensure the MySQL Database server, Kafka Server and Zookeeper are running.

### 1. Start Kafka Server
```bash
# Start Zookeeper (in a separate terminal) [Change path if kafka home path is different]
cd ~/kafka/kafka_2.13-3.8.1
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka server (in another terminal) [Change path if kafka home path is different]
cd ~/kafka/kafka_2.13-3.8.1
bin/kafka-server-start.sh config/server.properties
```

### 2. Create Kafka Topic
```bash
# Create the 'transactions' topic [Change path if kafka home path is different]
cd ~/kafka/kafka_2.13-3.8.1
bin/kafka-topics.sh --create --topic transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 3. Run Lambda Architecture Orchestration
```
python src/main.py
```

### To Query for the Serving Layer
```
python src/serving.py
```

## Project Structure
- `src/`
    - `main.py`: Main orchestration
    - `producer.py`: Implements kafka producer
    - `consumer.py`: Implements kafka consumer stream procesor
    - `batch_processor`: Implements batch processing
    - `serving.py`: Implements query of processed data
    - `mysql_controller`: For importing initial data into MySQL (only for testing)
- `data/`
    - `dataset_27/`
        - `cards.csv`: Contains data about cards
        - `credit_card_types.csv`: Contains data about all available credit card types
        - `customers.csv`: Contains data about all customers
        - `transactions.csv`: Contains all transaction data
    - `results/`
        - `batch_transactions.csv`: Contains transactions data after batch processing
        - `cards_updated.csv`: Contains cards data after all processing is done
        - `customers_updated.csv`: Contains customers data after all processing is done
        - `stream_transactions.csv`: Contains transactions data after stream processing
- `.env.example`: Template for environment variables
- `requirements.txt`: Python dependencies

## Whats Happening?
1. The provided data is imported into MySQL intitially.
2. The producer reads data from MySQL by query the transactions table by date and publishes records to the 'transactions' Kafka topic.
3. The consumer reads these messages and processes in time series, tracks card balances, approves or declines transactions based on validation rules.
4. After the stream layer processes required number of transactions (entire data), the batch process is triggered.
5. The batch process processes the entire batch of data. It validates the transactions, updates pending credit card balances, calculates new credit scores based on total credit usage across all cards, updates the user credit score and reduces the user credit limit if credit card score drops.
6. The final processed data is saved as csv files and the respetive tables are updated in the MySQL database.   

## Stopping the Services

To stop Kafka and Zookeeper:

```bash
# Stop Kafka
cd ~/kafka/kafka_2.13-3.8.1
bin/kafka-server-stop.sh

# Stop Zookeeper
bin/zookeeper-server-stop.sh
```