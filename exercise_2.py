from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import pyspark.sql.functions as F

"""

"""

def main(session: SparkSession):
    """
    For each seller, what is the average % contribution of an order to the seller's daily quota?
    # Example
    If Seller_0 with `quota=250` has 3 orders:
    Order 1: 10 products sold
    Order 2: 8 products sold
    Order 3: 7 products sold
    The average % contribution of orders to the seller's quota would be:
    Order 1: 10/105 = 0.04
    Order 2: 8/105 = 0.032
    Order 3: 7/105 = 0.028
    Average % Contribution = (0.04+0.032+0.028)/3 = 0.03333
    :param session:
    :return:
    """
    sales = session.read.parquet("data/sales_parquet")
    sellers = session.read.parquet("data/sellers_parquet")

    sales_sellers = sales.join(F.broadcast(sellers), on="seller_id", how="inner")

    quota_percentage = sales_sellers.\
        withColumn("quota_percentage", F.col("num_pieces_sold") / F.col("daily_target")). \
        select(["order_id", "quota_percentage"])

    quota_percentage.show()


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local") \
        .config("spark.sql.autoBroadcastJoinThreshold", -1) \
        .config("spark.executor.memory", "500mb") \
        .appName("Exercise2") \
        .getOrCreate()

    main(spark)
