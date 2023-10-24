from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext

def spark_process():
    spark = SparkSession \
     .builder \
     .appName("Orders Data Cassandra-to-Snowflake") \
     .config("spark.cassandra.connection.host", "cassandra") \
     .config("spark.cassandra.connection.port","9042")\
     .config("spark.cassandra.auth.username", "cassandra") \
     .config("spark.cassandra.auth.password", "cassandra") \
     .getOrCreate() 

#    
    schema_orders = StructType([
        StructField("event", StringType()),
        StructField("messageid", StringType()),
        StructField("userid", StringType()),
        StructField("properties", StringType()), 
        StructField("context", StringType()),
        StructField("timestamp", StringType()),
    ])

    df = spark.read\
        .format("org.apache.spark.sql.cassandra") \
        .options(table="view_products_table",keyspace="view_product_data_keyspace") \
        .option("schema",schema_orders) \
        .load()
    
    schema_properties = ArrayType(StructType([
       StructField("productid", StringType())
    ]))
    schema_context = ArrayType(StructType([
       StructField("source", StringType())
    ]))


    df_1 = df.withColumn("propertiesArray", from_json(df["properties"], schema_properties)) 

    df_2 = df_1.withColumn("contextArray", from_json(df["context"], schema_context))

#

    view_explode_properties_df = df_2.selectExpr("event", "messageid", "userid", "contextArray", "timestamp",
                                           "explode(propertiesArray) as properties")
    
    view_explode_context_df = view_explode_properties_df.selectExpr("event", "messageid", "userid", "properties", "timestamp",
                                           "explode(contextArray) as context")
    
    flattened_orders_df = view_explode_context_df.withColumn("productid", expr("properties.productid")) 

    flattened_orders_df_final = flattened_orders_df.withColumn("source", expr("context.source")) 
    


    select_orders_df = flattened_orders_df_final.select(col("timestamp"),col("event"), col("messageid"), col("userid"), col("productid"),
                                                  col("source")).alias("o")
    
    #check_point = 0
    try:
        with open('/opt/airflow/resources/check_point_timestamp.txt','r') as f:
            check_point = int(f.read())
    except FileNotFoundError:
        pass


    select_orders_df = select_orders_df.withColumn('timestamp', select_orders_df['timestamp'].cast('int'))


    fildered_data = select_orders_df.filter(select_orders_df.timestamp > check_point)

    fildered_data = fildered_data.withColumn('timestamp', select_orders_df['timestamp'].cast('string'))


    sfOptions = {
        "sfURL": "https://gekolij-jf87871.snowflakecomputing.com/",
        "sfUser": "dbt_retail",
        "sfPassword": "dbtPassword123",
        "sfDatabase": "VIEWS_ORDERS",
        "sfSchema": "RAW",
        "sfWarehouse": "RETAIL",
        "sfRole": "TRANSFORM"
        }
#
    fildered_data.write \
        .format("snowflake") \
        .options(**sfOptions) \
        .option("dbtable", "view_data") \
        .mode("append") \
        .save()

    check_point = fildered_data.agg({"timestamp": "max"}).collect()[0][0]
    
    print(check_point)
    with open("/opt/airflow/resources/check_point_timestamp.txt", "w") as f:
        f.write(str(check_point))

if __name__ == '__main__':
    spark_process()


        #.option("column_mapping", "name") \
        #.option("column_mismatch_behavior", "ignore") \