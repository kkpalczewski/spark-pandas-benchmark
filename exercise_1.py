from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def main(session: SparkSession):
    """
    What is the average revenue of the orders?
    :param session:
    :return:
    """
    products = session.read.parquet("data/products_parquet")
    sales = session.read.parquet("data/sales_parquet")

    sales_products = sales.join(products, on="product_id", how="inner")

    avg_revenue = sales_products.withColumn("revenue",
                                            F.col("price")*F.col("num_pieces_sold")).agg(F.avg("revenue"))

    avg_revenue.show()


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local") \
        .config("spark.sql.autoBroadcastJoinThreshold", -1) \
        .config("spark.executor.memory", "500mb") \
        .appName("Exercise1") \
        .getOrCreate()

    main(spark)
