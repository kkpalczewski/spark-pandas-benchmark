from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql import Window

def main(session: SparkSession):
    """
    Who are the second most selling and the least selling persons (sellers) for each product?
    Who are those for product with `product_id = 0`
    """
    sales = session.read.parquet("data/sales_parquet")

    sales = sales.\
        groupBy(["seller_id", "product_id"]).\
        agg(F.sum(F.col("num_pieces_sold").cast(IntegerType())).alias("sum_sales_per_seller"))


    window_asc = Window().\
        partitionBy("product_id").\
        orderBy(F.col("sum_sales_per_seller").asc())
    window_desc = Window(). \
        partitionBy("product_id").\
        orderBy(F.col("sum_sales_per_seller").desc())

    sales = sales.\
        withColumn("rank_asc", F.dense_rank().over(window_asc)).\
        withColumn("rank_desc", F.dense_rank().over(window_desc))

    second_most_selling = sales.\
        where(F.col("rank_asc") == 2).\
        select(["product_id", "seller_id"])

    least_selling = sales.\
        where(F.col("rank_desc") == 1).\
        select(["product_id", "seller_id"])

    second_most_selling.show()
    least_selling.show()


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local") \
        .config("spark.sql.autoBroadcastJoinThreshold", -1) \
        .config("spark.executor.memory", "3gb") \
        .appName("Exercise3") \
        .getOrCreate()

    main(spark)
