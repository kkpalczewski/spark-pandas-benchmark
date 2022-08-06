from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def main(session: SparkSession):
    """
    Find out how many orders, how many products and how many sellers are in the data.
    How many products have been sold at least once?
    Which is the product contained in more orders?
    :param session:
    :return:
    """
    products = session.read.parquet("data/products_parquet")
    sales = session.read.parquet("data/sales_parquet")
    sellers = session.read.parquet("data/sellers_parquet")

    products_count = products.count()
    sellers_count = sellers.count()

    products_sold = sales.select("product_id").distinct().count()

    products_most_orders = sales.\
        groupby("product_id").\
        count().\
        sort(F.col("count").desc()).\
        limit(1)
    print(f"Products count {products_count}, Sellers count: {sellers_count}!")
    print(f"Products sold at least once: {products_sold}!")
    print(f"Products most orders: {products_most_orders.take(1)}!")


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local") \
        .config("spark.sql.autoBroadcastJoinThreshold", -1) \
        .config("spark.executor.memory", "500mb") \
        .appName("Exercise1") \
        .getOrCreate()

    main(spark)
