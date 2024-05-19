from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType, TimestampType

if __name__ == '__main__':
    spark = SparkSession.builder.appName('ETL').master('local').config(
        "spark.jars", "mysql-connector-j-8.1.0.jar").getOrCreate()
    
    data = spark.read.option('header', True).csv('./ree_header.csv')

    print(data.schema.fields)

    for field in data.schema.fields:
        colname = field.name
        if colname != 'Date':
            data = data.withColumn(colname, col(colname).cast(FloatType()))
        else:
            data = data.withColumn(colname, col(colname).cast(TimestampType()))
    
    data.write.mode("overwrite").format("jdbc").option("driver","com.mysql.cj.jdbc.Driver").option(
        "url", "jdbc:mysql://172.17.0.3:3306/superset").option("dbtable", "test_spark").option(
            "user", "root").option("password", "my-secret-pw").save()