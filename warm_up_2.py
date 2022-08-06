from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def main(session: SparkSession):
    """
    How many distinct products have been sold in each day?
    :param session:
    :return:
    """
    products = session.read.parquet("data/products_parquet")
    sales = session.read.parquet("data/sales_parquet")

    distinct_sales = \
        sales.select(["product_id", "date"]).groupBy("date").agg(F.countDistinct("product_id"))

    print(distinct_sales.show())

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local") \
        .config("spark.sql.autoBroadcastJoinThreshold", -1) \
        .config("spark.executor.memory", "500mb") \
        .appName("WarmUp2") \
        .getOrCreate()

    main(spark)
