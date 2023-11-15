import pyspark
print(pyspark.__version__)

import logging
from datetime import datetime
import uuid
import time_uuid

import pandas as pd
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Build spark
def create_spark_connection():
    s_conn = None
    
    try:
        s_conn = SparkSession \
            .builder \
            .appName("SparkDataStreaming") \
            .config("spark.streaming.stopGracefullyOnShutdown", True) \
            .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
                                        "com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.3.0") \
            .config("spark.sql.shuffle.partitions", 4) \
            .config('spark.cassandra.connection.host', 'host.docker.internal') \
            .getOrCreate()
                
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

# Connect to kafka with spark connection
def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'kafka:29092') \
            .option('subscribe', 'user_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df



# Create pyspark dataframe from Kafka
## Define a function converting uuid to datetime (when records are updated)
def convert_uuid_to_datetime(uuid_string):
    uuid_object = uuid.UUID(uuid_string)
    timestamp = time_uuid.TimeUUID(bytes=uuid_object.bytes).get_timestamp()
    datetime_object = datetime.fromtimestamp(timestamp)
    return datetime_object

udf_convert_uuid_to_datetime = udf(lambda z: convert_uuid_to_datetime(z), TimestampType())

## Create pyspark dataframe
def create_selection_df_from_kafka(spark_df):
    # Define type for each columns
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")\
        .select(from_json(col('value'), schema).alias('data')).select("data.*")\
        .withColumn('dob', substring(col('dob'), 1, 19))\
        .withColumn('registered_date', substring(col('registered_date'), 1, 19))\
        .withColumn('update_date', udf_convert_uuid_to_datetime(col('id')))\
        .withColumn('update_date', substring(col('update_date'), 1, 19))
    return sel

# Create connection to cassandra
def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['host.docker.internal'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None

# Create keyspace in cassandra
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

# Create table in cassandra
def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        dob TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")


# def insert_data(session, **kwargs):
#     print("inserting data...")

#     user_id = kwargs.get('id')
#     first_name = kwargs.get('first_name')
#     last_name = kwargs.get('last_name')
#     gender = kwargs.get('gender')
#     address = kwargs.get('address')
#     postcode = kwargs.get('post_code')
#     email = kwargs.get('email')
#     username = kwargs.get('username')
#     dob = kwargs.get('dob')
#     registered_date = kwargs.get('registered_date')
#     phone = kwargs.get('phone')
#     picture = kwargs.get('picture')

#     try:
#         session.execute("""
#             INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
#                 post_code, email, username, dob, registered_date, phone, picture)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """, (user_id, first_name, last_name, gender, address,
#               postcode, email, username, dob, registered_date, phone, picture))
#         logging.info(f"Data inserted for {first_name} {last_name}")

#     except Exception as e:
#         logging.error(f'could not insert data due to {e}')

# main
if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            # insert_data(session)
            
            logging.info("Streaming is being started...")
            
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streaming_query.awaitTermination()