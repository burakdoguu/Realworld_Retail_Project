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


    schema_orders = StructType([
        StructField("event", StringType()),
        StructField("messageid", StringType()),
        StructField("userid", StringType()),
        StructField("lineitems", StringType()), 
        StructField("orderid", IntegerType()),
        StructField("timestamp", IntegerType()), 
    ])

    df = spark.read\
        .format("org.apache.spark.sql.cassandra") \
        .options(table="all_orders_table",keyspace="orders_data_keyspace") \
        .option("schema",schema_orders) \
        .load()
    
    schema_lineitems = ArrayType(StructType([
       StructField("productid", StringType()),
        StructField("quantity", IntegerType())
    ]))

    df = df.withColumn("lineItemsArray", from_json(df.lineitems, schema_lineitems))

    print(df.show())
    orders_explode_df = df.selectExpr("event", "messageid", "userid", "timestamp",
                                             "orderid", "explode(lineItemsArray) as lineItems")
    

    flattened_orders_df = orders_explode_df.withColumn("productid", expr("lineItems.productid")) \
        .withColumn("quantity", expr("lineItems.quantity")).drop("lineItems")
    
    
    select_orders_df = flattened_orders_df.select(col("event"), col("messageid"), col("userid"), col("orderid"),
                                                  col("productid"), col("quantity"), col("timestamp")).alias("o")
    
    select_orders_df.printSchema()

    print(select_orders_df.show())

    try:
        with open('/opt/airflow/resources/check_point2_timestamp.txt','r') as f:
            check_point = int(f.read())
    except FileNotFoundError:
        pass

    select_orders_df = select_orders_df.withColumn('timestamp', select_orders_df['timestamp'].cast('int'))

    fildered_data = select_orders_df.filter(select_orders_df.timestamp > check_point)

    fildered_data = fildered_data.withColumn('timestamp', select_orders_df['timestamp'].cast('string'))


    sfOptions = {
        "sfURL": "https://{your_account_name}.snowflakecomputing.com/",
        "sfUser": "dbt_retail",
        "sfPassword": "{your_password}",
        "sfDatabase": "VIEWS_ORDERS",
        "sfSchema": "RAW",
        "sfWarehouse": "RETAIL",
        "sfRole": "TRANSFORM"
        }

    fildered_data.write \
        .format("snowflake") \
        .options(**sfOptions) \
        .option("dbtable", "order_data") \
        .mode("append") \
        .save()
    
    check_point = fildered_data.agg({"timestamp": "max"}).collect()[0][0]
    
    print(check_point)
    with open("/opt/airflow/resources/check_point2_timestamp.txt", "w") as f:
        f.write(str(check_point))    

if __name__ == '__main__':
    spark_process()
