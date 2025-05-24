# import re
# import json
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
# import happybase
# from bs4 import BeautifulSoup

# # Create Spark Session
# def create_spark_session(app_name="StackSights"):
#     """Create a Spark session with necessary configurations"""
#     return (SparkSession.builder
#             .appName(app_name)
#             .config("spark.sql.streaming.checkpointLocation", "checkpoint")
#             .config("spark.jars.packages",
#                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,"
#                    "org.apache.hbase:hbase-client:2.4.13,"
#                    "org.apache.hbase:hbase-common:2.4.13")
#             .getOrCreate())

# # HTML Stripping Function
# def strip_html(html_text):
#     """Strip HTML tags from text using BeautifulSoup"""
#     if html_text is None:
#         return None
#     return BeautifulSoup(html_text, "html.parser").get_text()

# # Register UDF for HTML stripping
# def register_udfs(spark):
#     """Register UDFs for use in Spark SQL"""
#     # Register strip_html as UDF
#     spark.udf.register("strip_html", strip_html, StringType())

#     # UDF to check if a question has an accepted answer
#     def has_accepted_answer(answers):
#         if answers is None:
#             return False
#         for answer in answers:
#             if answer.get("is_accepted", False):
#                 return True
#         return False

#     spark.udf.register("has_accepted_answer", has_accepted_answer, BooleanType())

#     # UDF to check if a question is unanswered
#     def is_unanswered(answers):
#         return answers is None or len(answers) == 0

#     spark.udf.register("is_unanswered", is_unanswered, BooleanType())

#     # UDF to calculate average score of answers
#     def avg_answer_score(answers):
#         if answers is None or len(answers) == 0:
#             return 0.0
#         return sum(answer.get("score", 0) for answer in answers) / len(answers)

#     spark.udf.register("avg_answer_score", avg_answer_score, DoubleType())

#     # UDF to select top 3 answers
#     def select_top_answers(answers):
#         if answers is None or len(answers) == 0:
#             return []

#         # First find accepted answer
#         top_answers = []
#         accepted = None

#         for answer in answers:
#             if answer.get("is_accepted", False):
#                 accepted = answer
#                 break

#         if accepted:
#             top_answers.append(accepted)

#         # Then add highest scoring with reputation > 1000
#         high_rep_answers = [a for a in answers if
#                            a.get("owner_reputation", 0) > 1000 and
#                            a != accepted]
#         high_rep_answers.sort(key=lambda x: x["score"], reverse=True)

#         for answer in high_rep_answers:
#             if len(top_answers) >= 3:
#                 break
#             top_answers.append(answer)

#         # If still under 3, add highest scoring regardless of reputation
#         if len(top_answers) < 3:
#             remaining = [a for a in answers if a not in top_answers]
#             remaining.sort(key=lambda x: x["score"], reverse=True)

#             for answer in remaining:
#                 if len(top_answers) >= 3:
#                     break
#                 top_answers.append(answer)

#         return top_answers

#     # We'll use this in the foreach batch writer rather than as a SQL UDF
#     return select_top_answers

# # Define schema for the questions topic
# def get_questions_schema():
#     """Define the schema for the Stack Overflow questions Kafka topic"""
#     answer_schema = StructType([
#         StructField("answer_id", LongType(), True),
#         StructField("body", StringType(), True),
#         StructField("score", IntegerType(), True),
#         StructField("is_accepted", BooleanType(), True),
#         StructField("owner_reputation", IntegerType(), True)
#     ])

#     return StructType([
#         StructField("question_id", LongType(), True),
#         StructField("title", StringType(), True),
#         StructField("body", StringType(), True),
#         StructField("creation_date", LongType(), True),
#         StructField("score", IntegerType(), True),
#         StructField("tags", ArrayType(StringType()), True),
#         StructField("owner_reputation", IntegerType(), True),
#         StructField("answers", ArrayType(answer_schema), True)
#     ])

# # Define schema for the trends topic
# def get_trends_schema():
#     """Define the schema for the Stack Overflow trends Kafka topic"""
#     return StructType([
#         StructField("tag", StringType(), True),
#         StructField("count", IntegerType(), True)
#     ])

# # Process questions stream
# def process_questions_stream(spark, kafka_bootstrap_servers="localhost:29092", select_top_answers_fn=None):
#     """Process the Stack Overflow questions stream from Kafka"""
#     # Read from Kafka
#     questions_df = (spark
#         .readStream
#         .format("kafka")
#         .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
#         .option("subscribe", "stackoverflow-questions")
#         .option("startingOffsets", "latest")
#         .load()
#         .selectExpr("CAST(value AS STRING) as json_data")
#         .select(from_json("json_data", get_questions_schema()).alias("data"))
#         .select("data.*")
#     )

#     # Strip HTML from question body
#     questions_df = questions_df.withColumn("body", expr("strip_html(body)"))

#     # Strip HTML from answers
#     questions_df = questions_df.withColumn(
#         "answers",
#         expr("""
#             transform(answers, a ->
#                 named_struct(
#                     'answer_id', a.answer_id,
#                     'body', strip_html(a.body),
#                     'score', a.score,
#                     'is_accepted', a.is_accepted,
#                     'owner_reputation', a.owner_reputation
#                 )
#             )
#         """)
#     )

#     # Add useful flags
#     questions_df = questions_df.withColumn("has_accepted_answer", expr("has_accepted_answer(answers)"))
#     questions_df = questions_df.withColumn("is_unanswered", expr("is_unanswered(answers)"))
#     questions_df = questions_df.withColumn("avg_answer_score", expr("avg_answer_score(answers)"))

#     # Add timestamp column for windowing
#     questions_df = questions_df.withColumn(
#         "event_time",
#         from_unixtime(col("creation_date")).cast("timestamp")
#     )

#     # Setup foreach batch writer to HBase
#     def write_questions_to_hbase(batch_df, batch_id):
#         if batch_df.isEmpty():
#             return

#         # Process and write to HBase
#         questions = batch_df.collect()

#         # Connect to HBase
#         connection = happybase.Connection('localhost')
#         qna_table = connection.table('stackoverflow_qna')
#         tag_index_table = connection.table('stackoverflow_tag_index')

#         for question in questions:
#             # Convert to Python dict
#             question_dict = question.asDict()

#             # Select top answers
#             if select_top_answers_fn:
#                 top_answers = select_top_answers_fn(question_dict.get('answers', []))
#             else:
#                 top_answers = []

#             # Convert to HBase format
#             question_id = str(question_dict['question_id'])

#             # Prepare data for the question column family
#             question_data = {
#                 b'question:title': str(question_dict['title']).encode(),
#                 b'question:body': str(question_dict['body']).encode(),
#                 b'question:creation_date': str(question_dict['creation_date']).encode(),
#                 b'question:score': str(question_dict['score']).encode(),
#                 b'question:owner_reputation': str(question_dict['owner_reputation']).encode(),
#                 b'question:tags': json.dumps(question_dict['tags']).encode(),
#                 b'question:has_accepted': str(question_dict['has_accepted_answer']).encode(),
#                 b'question:is_unanswered': str(question_dict['is_unanswered']).encode()
#             }

#             # Add answers
#             answers = question_dict.get('answers', [])
#             for i, answer in enumerate(answers, 1):
#                 question_data[f'answers:answer{i}_id'.encode()] = str(answer['answer_id']).encode()
#                 question_data[f'answers:answer{i}_body'.encode()] = str(answer['body']).encode()
#                 question_data[f'answers:answer{i}_score'.encode()] = str(answer['score']).encode()
#                 question_data[f'answers:answer{i}_is_accepted'.encode()] = str(answer.get('is_accepted', False)).encode()
#                 question_data[f'answers:answer{i}_owner_reputation'.encode()] = str(answer.get('owner_reputation', 0)).encode()

#             # Add top answers
#             for i, answer in enumerate(top_answers, 1):
#                 question_data[f'top_answers:top{i}_id'.encode()] = str(answer['answer_id']).encode()
#                 question_data[f'top_answers:top{i}_body'.encode()] = str(answer['body']).encode()
#                 question_data[f'top_answers:top{i}_score'.encode()] = str(answer['score']).encode()
#                 question_data[f'top_answers:top{i}_is_accepted'.encode()] = str(answer.get('is_accepted', False)).encode()
#                 question_data[f'top_answers:top{i}_owner_reputation'.encode()] = str(answer.get('owner_reputation', 0)).encode()

#             # Insert into HBase
#             qna_table.put(question_id.encode(), question_data)

#             # Update tag index
#             creation_date = str(question_dict['creation_date'])
#             for tag in question_dict['tags']:
#                 tag_index_table.put(
#                     tag.encode(),
#                     {f'question_ids:{creation_date}'.encode(): question_id.encode()}
#                 )

#     # Explode tags for aggregation
#     tagged_questions = questions_df.withColumn("tag", explode("tags"))

#     # Create hourly tumbling window with watermark
#     windowed_counts = (tagged_questions
#         .withWatermark("event_time", "10 minutes")
#         .groupBy(
#             "tag",
#             window("event_time", "1 hour")
#         )
#         .agg(
#             count("*").alias("total_questions"),
#             (avg(when(col("is_unanswered"), 1).otherwise(0)) * 100).alias("unanswered_percent"),
#             (avg(when(col("has_accepted_answer"), 1).otherwise(0)) * 100).alias("accepted_percent"),
#             avg("score").alias("avg_question_score"),
#             avg("avg_answer_score").alias("avg_answer_score")
#         )
#     )

#     # Write aggregated metrics to HBase
#     def write_hourly_metrics_to_hbase(batch_df, batch_id):
#         if batch_df.isEmpty():
#             return

#         # Connect to HBase
#         connection = happybase.Connection('localhost')
#         trends_table = connection.table('stackoverflow_trends')

#         # Process each row
#         metrics = batch_df.collect()
#         for metric in metrics:
#             # Convert to Python dict
#             metric_dict = metric.asDict()

#             # Extract window information
#             window_dict = metric_dict['window']
#             window_start = window_dict['start']
#             window_end = window_dict['end']

#             # Format timestamp for row key
#             timestamp = window_start.strftime('%Y%m%d%H')

#             # Create row key
#             tag = metric_dict['tag']
#             row_key = f"{tag}#hourly#{timestamp}"

#             # Prepare data
#             trend_data = {
#                 b'trend:total_questions': str(metric_dict['total_questions']).encode(),
#                 b'trend:unanswered_percent': str(metric_dict['unanswered_percent']).encode(),
#                 b'trend:accepted_percent': str(metric_dict['accepted_percent']).encode(),
#                 b'trend:avg_question_score': str(metric_dict['avg_question_score']).encode(),
#                 b'trend:avg_answer_score': str(metric_dict['avg_answer_score']).encode()
#             }

#             # Insert into HBase
#             trends_table.put(row_key.encode(), trend_data)

#     # Start both streams
#     query1 = (questions_df.writeStream
#         .foreachBatch(write_questions_to_hbase)
#         .outputMode("update")
#         .trigger(processingTime="5 minutes")
#         .start())

#     query2 = (windowed_counts.writeStream
#         .foreachBatch(write_hourly_metrics_to_hbase)
#         .outputMode("update")
#         .trigger(processingTime="5 minutes")
#         .start())

#     return [query1, query2]

# # Process trends stream
# def process_trends_stream(spark, kafka_bootstrap_servers="localhost:29092"):
#     """Process the Stack Overflow trends stream from Kafka"""
#     # Read from Kafka
#     trends_df = (spark
#         .readStream
#         .format("kafka")
#         .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
#         .option("subscribe", "stackoverflow-trends")
#         .option("startingOffsets", "latest")
#         .load()
#         .selectExpr("CAST(value AS STRING) as json_data", "CURRENT_TIMESTAMP() as event_time")
#         .select(from_json("json_data", get_trends_schema()).alias("data"), "event_time")
#         .select("data.*", "event_time")
#     )

#     # Write trends to HBase
#     def write_trends_to_hbase(batch_df, batch_id):
#         if batch_df.isEmpty():
#             return

#         # Connect to HBase
#         connection = happybase.Connection('localhost')
#         trends_table = connection.table('stackoverflow_trends')

#         # Process each row
#         trends = batch_df.collect()
#         for trend in trends:
#             # Convert to Python dict
#             trend_dict = trend.asDict()

#             # Get current time for all time frames
#             timestamp = trend.event_time

#             # Create row keys for different time periods
#             tag = trend_dict['tag']
#             count = trend_dict['count']

#             # Hourly key
#             hourly_timestamp = timestamp.strftime('%Y%m%d%H')
#             hourly_key = f"{tag}#hourly#{hourly_timestamp}"

#             # Daily key
#             daily_timestamp = timestamp.strftime('%Y%m%d')
#             daily_key = f"{tag}#daily#{daily_timestamp}"

#             # Monthly key
#             monthly_timestamp = timestamp.strftime('%Y%m')
#             monthly_key = f"{tag}#monthly#{monthly_timestamp}"

#             # Common data for all periods
#             trend_data = {
#                 b'trend:raw_count': str(count).encode()
#             }

#             # Insert into HBase for all periods
#             for key in [hourly_key, daily_key, monthly_key]:
#                 trends_table.put(key.encode(), trend_data)

#     # Start the stream
#     query = (trends_df.writeStream
#         .foreachBatch(write_trends_to_hbase)
#         .outputMode("update")
#         .trigger(processingTime="5 minutes")
#         .start())

#     return query

# # Daily and monthly batch aggregation
# def run_batch_aggregations(spark):
#     """Run daily and monthly aggregations as batch jobs"""
#     # Create batch SQL view from HBase
#     # Note: This would typically require setting up a HBase catalog and
#     # using the HBase connector for Spark. Below is a simplified example.

#     # For a production system, you would:
#     # 1. Use spark-hbase connector to read from HBase
#     # 2. Create temporary views for daily/monthly aggregations
#     # 3. Run SQL to compute aggregations
#     # 4. Write results back to HBase

#     # This function would be scheduled to run daily/monthly
#     pass

# # Main function
# def main():
#     """Main function to start the Spark Streaming pipeline"""
#     # Create Spark session
#     spark = create_spark_session()

#     # Register UDFs
#     select_top_answers = register_udfs(spark)

#     # Start streaming queries
#     queries = []
#     queries.extend(process_questions_stream(spark, select_top_answers_fn=select_top_answers))
#     queries.append(process_trends_stream(spark))

#     # Keep the application running
#     spark.streams.awaitAnyTermination()

# if __name__ == "__main__":
#     main()


#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Spark-HBase Integration for Stack Overflow Data Processing
---------------------------------------------------------

This script implements the Spark Structured Streaming pipeline to:
1. Consume Kafka topics with Stack Overflow data
2. Strip HTML from question and answer bodies
3. Process and aggregate question/answer data
4. Write processed data to HBase

Requirements from the specification:
- Use Spark 3.2 Structured Streaming
- Define a 1-hour tumbling window with 10-minute watermark
- Maintain daily and monthly aggregates
- Strip HTML tags from body fields
- Compute tag-based metrics
- Write results to HBase tables
"""
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Spark-HBase Integration for Stack Overflow Data Processing (PY4J FIX)
-------------------------------------------------------------------

This script fixes the Py4J error in the Spark Structured Streaming pipeline by:
1. Adding proper exception handling in foreachBatch functions
2. Improving memory management for large datasets
3. Ensuring HBase connections are properly managed
4. Adding configurable timeout and retry mechanisms

The script maintains all the previous functionality:
- Consuming from Kafka topics
- HTML stripping from bodies
- Processing and aggregating data
- Writing to HBase with human-readable dates
"""

import re
import json
import time
import datetime
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import happybase
from bs4 import BeautifulSoup

# Configuration
HBASE_HOST = "localhost"
HBASE_PORT = 9090
HBASE_BATCH_SIZE = 10  # Number of records to batch in HBase operations
HBASE_CONNECTION_TIMEOUT = 30000  # milliseconds
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


# Create Spark Session with proper memory configuration
def create_spark_session(app_name="StackSights"):
    """Create a Spark session with necessary configurations"""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.streaming.checkpointLocation", "checkpoint")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,"
            "org.apache.hbase:hbase-client:2.4.13,"
            "org.apache.hbase:hbase-common:2.4.13",
        )
        # Add memory configuration to prevent OOM errors
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2g")
        # Add Py4J configurations to prevent timeouts
        .config("spark.python.worker.reuse", "true")
        .config("spark.python.worker.memory", "1g")
        .config("spark.executor.heartbeatInterval", "20s")
        .config("spark.network.timeout", "300s")
        .getOrCreate()
    )


# HBase Connection Management
class HBaseConnectionManager:
    """Manage HBase connections to ensure proper resource handling"""

    def __init__(
        self, host=HBASE_HOST, port=HBASE_PORT, timeout=HBASE_CONNECTION_TIMEOUT
    ):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.connection = None

    def get_connection(self):
        """Get a connection to HBase, creating a new one if needed"""
        if self.connection is None:
            try:
                self.connection = happybase.Connection(
                    host=self.host, port=self.port, timeout=self.timeout
                )
            except Exception as e:
                print(f"Error creating HBase connection: {e}")
                raise
        return self.connection

    def get_table(self, table_name):
        """Get a table reference with proper error handling"""
        try:
            connection = self.get_connection()
            return connection.table(table_name)
        except Exception as e:
            print(f"Error getting HBase table {table_name}: {e}")
            # Try to refresh connection on error
            self.close()
            self.connection = None
            connection = self.get_connection()
            return connection.table(table_name)

    def close(self):
        """Close the connection if it exists"""
        if self.connection is not None:
            try:
                self.connection.close()
            except Exception as e:
                print(f"Error closing HBase connection: {e}")
            finally:
                self.connection = None


# HTML Stripping Function
def strip_html_completely(html_text):
    """
    Strip HTML tags completely from text using BeautifulSoup

    This function ensures all HTML tags are removed, returning only the text content.
    """
    if html_text is None or html_text == "":
        return ""

    try:
        # Parse with BeautifulSoup and extract text
        soup = BeautifulSoup(html_text, "html.parser")
        text = soup.get_text(separator=" ", strip=True)

        # Additional cleanup for any remaining HTML-like content
        text = re.sub(r"<[^>]+>", "", text)

        # Remove excessive whitespace
        text = re.sub(r"\s+", " ", text).strip()

        return text
    except Exception as e:
        print(f"Error stripping HTML: {e}")
        # Return a safe version of the text
        if html_text:
            return re.sub(r"<[^>]+>", "", html_text)
        return ""


# Format timestamp to human-readable date
def format_timestamp(timestamp):
    """Convert Unix timestamp to human-readable date format"""
    if timestamp is None:
        return ""

    try:
        # Convert to integer if it's a string
        if isinstance(timestamp, str):
            timestamp = int(timestamp)

        # Convert timestamp to datetime and format
        dt = datetime.datetime.fromtimestamp(timestamp)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError) as e:
        print(f"Error formatting timestamp {timestamp}: {e}")
        return str(timestamp)  # Return original as fallback


# Register UDFs for Spark SQL
def register_udfs(spark):
    """Register UDFs for use in Spark SQL"""
    # Register strip_html_completely as UDF
    spark.udf.register("strip_html_completely", strip_html_completely, StringType())

    # Register format_timestamp as UDF
    spark.udf.register("format_timestamp", format_timestamp, StringType())

    # UDF to check if a question has an accepted answer
    def has_accepted_answer(answers):
        if answers is None:
            return False
        for answer in answers:
            if answer.get("is_accepted", False):
                return True
        return False

    spark.udf.register("has_accepted_answer", has_accepted_answer, BooleanType())

    # UDF to check if a question is unanswered
    def is_unanswered(answers):
        return answers is None or len(answers) == 0

    spark.udf.register("is_unanswered", is_unanswered, BooleanType())

    # UDF to calculate average score of answers
    def avg_answer_score(answers):
        if answers is None or len(answers) == 0:
            return 0.0
        return sum(answer.get("score", 0) for answer in answers) / len(answers)

    spark.udf.register("avg_answer_score", avg_answer_score, DoubleType())

    # UDF to select top 3 answers - with error handling
    def select_top_answers(answers):
        if answers is None or len(answers) == 0:
            return []

        try:
            # First find accepted answer
            top_answers = []
            accepted = None

            for answer in answers:
                if answer.get("is_accepted", False):
                    accepted = answer
                    break

            if accepted:
                top_answers.append(accepted)

            # Then add highest scoring with reputation > 1000
            high_rep_answers = [
                a
                for a in answers
                if a.get("owner_reputation", 0) > 1000 and a != accepted
            ]
            high_rep_answers.sort(key=lambda x: x["score"], reverse=True)

            for answer in high_rep_answers:
                if len(top_answers) >= 3:
                    break
                top_answers.append(answer)

            # If still under 3, add highest scoring regardless of reputation
            if len(top_answers) < 3:
                remaining = [a for a in answers if a not in top_answers]
                remaining.sort(key=lambda x: x["score"], reverse=True)

                for answer in remaining:
                    if len(top_answers) >= 3:
                        break
                    top_answers.append(answer)

            return top_answers

        except Exception as e:
            print(f"Error in select_top_answers: {e}")
            # Return a safe result on error
            return answers[: min(3, len(answers))]

    # We'll use this in the foreach batch writer rather than as a SQL UDF
    return select_top_answers


# Define schema for the questions topic
def get_questions_schema():
    """Define the schema for the Stack Overflow questions Kafka topic"""
    answer_schema = StructType(
        [
            StructField("answer_id", LongType(), True),
            StructField("body", StringType(), True),
            StructField("score", IntegerType(), True),
            StructField("is_accepted", BooleanType(), True),
            StructField("owner_reputation", IntegerType(), True),
        ]
    )

    return StructType(
        [
            StructField("question_id", LongType(), True),
            StructField("title", StringType(), True),
            StructField("body", StringType(), True),
            StructField("creation_date", LongType(), True),
            StructField("score", IntegerType(), True),
            StructField("tags", ArrayType(StringType()), True),
            StructField("owner_reputation", IntegerType(), True),
            StructField("answers", ArrayType(answer_schema), True),
        ]
    )


# Define schema for the trends topic
def get_trends_schema():
    """Define the schema for the Stack Overflow trends Kafka topic"""
    return StructType(
        [
            StructField("tag", StringType(), True),
            StructField("count", IntegerType(), True),
        ]
    )


# Process questions stream
def process_questions_stream(
    spark, kafka_bootstrap_servers="localhost:29092", select_top_answers_fn=None
):
    """Process the Stack Overflow questions stream from Kafka"""
    # Read from Kafka
    questions_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", "stackoverflow-questions")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")  # Don't fail if data is deleted from Kafka
        .option("maxOffsetsPerTrigger", 1000)  # Limit batch size
        .load()
        .selectExpr("CAST(value AS STRING) as json_data")
        .select(from_json("json_data", get_questions_schema()).alias("data"))
        .select("data.*")
    )

    # Strip HTML from question body using the improved function
    questions_df = questions_df.withColumn("body", expr("strip_html_completely(body)"))

    # Format creation_date as human-readable
    questions_df = questions_df.withColumn(
        "creation_date_readable", expr("format_timestamp(creation_date)")
    )

    # Strip HTML from answers
    questions_df = questions_df.withColumn(
        "answers",
        expr(
            """
            transform(answers, a -> 
                named_struct(
                    'answer_id', a.answer_id,
                    'body', strip_html_completely(a.body),
                    'score', a.score,
                    'is_accepted', a.is_accepted,
                    'owner_reputation', a.owner_reputation
                )
            )
        """
        ),
    )

    # Add useful flags
    questions_df = questions_df.withColumn(
        "has_accepted_answer", expr("has_accepted_answer(answers)")
    )
    questions_df = questions_df.withColumn(
        "is_unanswered", expr("is_unanswered(answers)")
    )
    questions_df = questions_df.withColumn(
        "avg_answer_score", expr("avg_answer_score(answers)")
    )

    # Add timestamp column for windowing
    questions_df = questions_df.withColumn(
        "event_time", from_unixtime(col("creation_date")).cast("timestamp")
    )

    # Setup foreach batch writer to HBase with proper error handling
    def write_questions_to_hbase(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # Create a new connection manager for this batch
        hbase_manager = HBaseConnectionManager()

        try:
            # Process and write to HBase
            questions = batch_df.collect()
            print(f"Processing batch {batch_id} with {len(questions)} questions")

            # Get table references
            qna_table = hbase_manager.get_table("stackoverflow_qna")
            tag_index_table = hbase_manager.get_table("stackoverflow_tag_index")

            # Process in smaller batches to avoid memory issues
            for i in range(0, len(questions), HBASE_BATCH_SIZE):
                batch_slice = questions[i : i + HBASE_BATCH_SIZE]
                print(
                    f"Processing sub-batch {i//HBASE_BATCH_SIZE + 1} with {len(batch_slice)} questions"
                )

                for question in batch_slice:
                    # Use retries for robustness
                    for retry in range(MAX_RETRIES):
                        try:
                            # Convert to Python dict
                            question_dict = question.asDict()

                            # Select top answers
                            if select_top_answers_fn:
                                top_answers = select_top_answers_fn(
                                    question_dict.get("answers", [])
                                )
                            else:
                                top_answers = []

                            # Convert to HBase format
                            question_id = str(question_dict["question_id"])

                            # Prepare data for the question column family
                            question_data = {
                                b"question:title": str(question_dict["title"]).encode(),
                                b"question:body": str(question_dict["body"]).encode(),
                                b"question:creation_date": str(
                                    question_dict.get("creation_date_readable", "")
                                ).encode(),
                                b"question:raw_creation_date": str(
                                    question_dict["creation_date"]
                                ).encode(),
                                b"question:score": str(question_dict["score"]).encode(),
                                b"question:owner_reputation": str(
                                    question_dict["owner_reputation"]
                                ).encode(),
                                b"question:tags": json.dumps(
                                    question_dict["tags"]
                                ).encode(),
                                b"question:has_accepted": str(
                                    question_dict["has_accepted_answer"]
                                ).encode(),
                                b"question:is_unanswered": str(
                                    question_dict["is_unanswered"]
                                ).encode(),
                            }

                            # Add answers with properly stripped HTML
                            answers = question_dict.get("answers", [])
                            for j, answer in enumerate(answers, 1):
                                question_data[f"answers:answer{j}_id".encode()] = str(
                                    answer["answer_id"]
                                ).encode()
                                question_data[f"answers:answer{j}_body".encode()] = str(
                                    answer["body"]
                                ).encode()
                                question_data[f"answers:answer{j}_score".encode()] = (
                                    str(answer["score"]).encode()
                                )
                                question_data[
                                    f"answers:answer{j}_is_accepted".encode()
                                ] = str(answer.get("is_accepted", False)).encode()
                                question_data[
                                    f"answers:answer{j}_owner_reputation".encode()
                                ] = str(answer.get("owner_reputation", 0)).encode()

                            # Add top answers with properly stripped HTML
                            for j, answer in enumerate(top_answers, 1):
                                question_data[f"top_answers:top{j}_id".encode()] = str(
                                    answer["answer_id"]
                                ).encode()
                                question_data[f"top_answers:top{j}_body".encode()] = (
                                    str(answer["body"]).encode()
                                )
                                question_data[f"top_answers:top{j}_score".encode()] = (
                                    str(answer["score"]).encode()
                                )
                                question_data[
                                    f"top_answers:top{j}_is_accepted".encode()
                                ] = str(answer.get("is_accepted", False)).encode()
                                question_data[
                                    f"top_answers:top{j}_owner_reputation".encode()
                                ] = str(answer.get("owner_reputation", 0)).encode()

                            # Insert into HBase
                            qna_table.put(question_id.encode(), question_data)

                            # Update tag index
                            creation_date = str(question_dict["creation_date"])
                            for tag in question_dict["tags"]:
                                tag_index_table.put(
                                    tag.encode(),
                                    {
                                        f"question_ids:{creation_date}".encode(): question_id.encode()
                                    },
                                )

                            # Success, break retry loop
                            break

                        except Exception as e:
                            print(
                                f"Error processing question {question_dict['question_id']} (attempt {retry+1}/{MAX_RETRIES}): {e}"
                            )
                            traceback.print_exc()

                            if retry < MAX_RETRIES - 1:
                                # Try to refresh connection before retry
                                hbase_manager.close()
                                time.sleep(RETRY_DELAY)
                                qna_table = hbase_manager.get_table("stackoverflow_qna")
                                tag_index_table = hbase_manager.get_table(
                                    "stackoverflow_tag_index"
                                )
                            else:
                                print(
                                    f"Failed to process question after {MAX_RETRIES} attempts"
                                )

        except Exception as e:
            print(f"Error in write_questions_to_hbase for batch {batch_id}: {e}")
            traceback.print_exc()
        finally:
            # Always close the connection when done
            hbase_manager.close()

    # Explode tags for aggregation
    tagged_questions = questions_df.withColumn("tag", explode("tags"))

    # Create hourly tumbling window with watermark
    windowed_counts = (
        tagged_questions.withWatermark("event_time", "10 minutes")
        .groupBy("tag", window("event_time", "1 hour"))
        .agg(
            count("*").alias("total_questions"),
            (avg(when(col("is_unanswered"), 1).otherwise(0)) * 100).alias(
                "unanswered_percent"
            ),
            (avg(when(col("has_accepted_answer"), 1).otherwise(0)) * 100).alias(
                "accepted_percent"
            ),
            avg("score").alias("avg_question_score"),
            avg("avg_answer_score").alias("avg_answer_score"),
        )
    )

    # Write aggregated metrics to HBase with proper error handling
    def write_hourly_metrics_to_hbase(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # Create a new connection manager for this batch
        hbase_manager = HBaseConnectionManager()

        try:
            # Process each row
            metrics = batch_df.collect()
            print(f"Processing metrics batch {batch_id} with {len(metrics)} records")

            # Get table reference
            trends_table = hbase_manager.get_table("stackoverflow_trends")

            # Process in smaller batches
            for i in range(0, len(metrics), HBASE_BATCH_SIZE):
                batch_slice = metrics[i : i + HBASE_BATCH_SIZE]

                for metric in batch_slice:
                    # Use retries for robustness
                    for retry in range(MAX_RETRIES):
                        try:
                            # Convert to Python dict
                            metric_dict = metric.asDict()

                            # Extract window information
                            window_dict = metric_dict["window"]
                            window_start = window_dict["start"]
                            window_end = window_dict["end"]

                            # Format timestamp for row key
                            timestamp = window_start.strftime("%Y%m%d%H")

                            # Create row key
                            tag = metric_dict["tag"]
                            row_key = f"{tag}#hourly#{timestamp}"

                            # Prepare data with formatted timestamp
                            trend_data = {
                                b"trend:total_questions": str(
                                    metric_dict["total_questions"]
                                ).encode(),
                                b"trend:unanswered_percent": str(
                                    metric_dict["unanswered_percent"]
                                ).encode(),
                                b"trend:accepted_percent": str(
                                    metric_dict["accepted_percent"]
                                ).encode(),
                                b"trend:avg_question_score": str(
                                    metric_dict["avg_question_score"]
                                ).encode(),
                                b"trend:avg_answer_score": str(
                                    metric_dict["avg_answer_score"]
                                ).encode(),
                                b"trend:timestamp_readable": window_start.strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ).encode(),
                            }

                            # Insert into HBase
                            trends_table.put(row_key.encode(), trend_data)

                            # Success, break retry loop
                            break

                        except Exception as e:
                            print(
                                f"Error processing metric for tag {metric_dict['tag']} (attempt {retry+1}/{MAX_RETRIES}): {e}"
                            )

                            if retry < MAX_RETRIES - 1:
                                # Try to refresh connection before retry
                                hbase_manager.close()
                                time.sleep(RETRY_DELAY)
                                trends_table = hbase_manager.get_table(
                                    "stackoverflow_trends"
                                )
                            else:
                                print(
                                    f"Failed to process metric after {MAX_RETRIES} attempts"
                                )

        except Exception as e:
            print(f"Error in write_hourly_metrics_to_hbase for batch {batch_id}: {e}")
            traceback.print_exc()
        finally:
            # Always close the connection when done
            hbase_manager.close()

    # Start both streams with appropriate error handling
    query1 = (
        questions_df.writeStream.foreachBatch(write_questions_to_hbase)
        .outputMode("update")
        .trigger(processingTime="5 minutes")
        .start()
    )

    query2 = (
        windowed_counts.writeStream.foreachBatch(write_hourly_metrics_to_hbase)
        .outputMode("update")
        .trigger(processingTime="5 minutes")
        .start()
    )

    return [query1, query2]


# Process trends stream
def process_trends_stream(spark, kafka_bootstrap_servers="localhost:29092"):
    """Process the Stack Overflow trends stream from Kafka"""
    # Read from Kafka
    trends_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", "stackoverflow-trends")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")  # Don't fail if data is deleted from Kafka
        .option("maxOffsetsPerTrigger", 1000)  # Limit batch size
        .load()
        .selectExpr(
            "CAST(value AS STRING) as json_data", "CURRENT_TIMESTAMP() as event_time"
        )
        .select(from_json("json_data", get_trends_schema()).alias("data"), "event_time")
        .select("data.*", "event_time")
    )

    # Write trends to HBase with proper error handling
    def write_trends_to_hbase(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # Create a new connection manager for this batch
        hbase_manager = HBaseConnectionManager()

        try:
            # Process each row
            trends = batch_df.collect()
            print(f"Processing trends batch {batch_id} with {len(trends)} records")

            # Get table reference
            trends_table = hbase_manager.get_table("stackoverflow_trends")

            # Process in smaller batches
            for i in range(0, len(trends), HBASE_BATCH_SIZE):
                batch_slice = trends[i : i + HBASE_BATCH_SIZE]

                for trend in batch_slice:
                    # Use retries for robustness
                    for retry in range(MAX_RETRIES):
                        try:
                            # Convert to Python dict
                            trend_dict = trend.asDict()

                            # Get current time for all time frames
                            timestamp = trend.event_time
                            timestamp_readable = timestamp.strftime("%Y-%m-%d %H:%M:%S")

                            # Create row keys for different time periods
                            tag = trend_dict["tag"]
                            count = trend_dict["count"]

                            # Hourly key
                            hourly_timestamp = timestamp.strftime("%Y%m%d%H")
                            hourly_key = f"{tag}#hourly#{hourly_timestamp}"

                            # Daily key
                            daily_timestamp = timestamp.strftime("%Y%m%d")
                            daily_key = f"{tag}#daily#{daily_timestamp}"

                            # Monthly key
                            monthly_timestamp = timestamp.strftime("%Y%m")
                            monthly_key = f"{tag}#monthly#{monthly_timestamp}"

                            # Common data for all periods
                            trend_data = {
                                b"trend:raw_count": str(count).encode(),
                                b"trend:timestamp_readable": timestamp_readable.encode(),
                            }

                            # Insert into HBase for all periods
                            for key in [hourly_key, daily_key, monthly_key]:
                                trends_table.put(key.encode(), trend_data)

                            # Success, break retry loop
                            break

                        except Exception as e:
                            print(
                                f"Error processing trend for tag {trend_dict['tag']} (attempt {retry+1}/{MAX_RETRIES}): {e}"
                            )

                            if retry < MAX_RETRIES - 1:
                                # Try to refresh connection before retry
                                hbase_manager.close()
                                time.sleep(RETRY_DELAY)
                                trends_table = hbase_manager.get_table(
                                    "stackoverflow_trends"
                                )
                            else:
                                print(
                                    f"Failed to process trend after {MAX_RETRIES} attempts"
                                )

        except Exception as e:
            print(f"Error in write_trends_to_hbase for batch {batch_id}: {e}")
            traceback.print_exc()
        finally:
            # Always close the connection when done
            hbase_manager.close()

    # Start the stream
    query = (
        trends_df.writeStream.foreachBatch(write_trends_to_hbase)
        .outputMode("update")
        .trigger(processingTime="5 minutes")
        .start()
    )

    return query


# Main function
def main():
    """Main function to start the Spark Streaming pipeline"""
    # Create Spark session
    spark = create_spark_session()

    # Register UDFs
    select_top_answers = register_udfs(spark)

    # Start streaming queries
    queries = []

    try:
        # Start the streams with proper error handling
        print("Starting questions stream...")
        question_queries = process_questions_stream(
            spark, select_top_answers_fn=select_top_answers
        )
        queries.extend(question_queries)

        print("Starting trends stream...")
        trend_query = process_trends_stream(spark)
        queries.append(trend_query)

        print("All streams started successfully")

        # Keep the application running
        spark.streams.awaitAnyTermination()
    except Exception as e:
        print(f"Error starting streams: {e}")
        traceback.print_exc()
    finally:
        # Ensure all queries are stopped properly
        for query in queries:
            try:
                if query.isActive:
                    query.stop()
            except:
                pass


if __name__ == "__main__":
    main()
