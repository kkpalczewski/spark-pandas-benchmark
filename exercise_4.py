from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql import Window, Column
import hashlib

def main(session: SparkSession):
    """
    Create a new column called "hashed_bill" defined as follows:
    - if the order_id is even: apply MD5 hashing iteratively to the
        bill_raw_text field, once for each 'A' (capital 'A') present in the text.
        E.g. if the bill text is 'nbAAnllA', you would apply hashing three times
        iteratively (only if the order number is even)
    - if the order_id is odd: apply SHA256 hashing to the bill text
    Finally, check if there are any duplicate on the new column

    Strategy:
    1. Count capital 'A'
    2. Create function for even 'A' count
    3. Create function for odd 'A' count
    4. Apply functions
    5. Check for duplicates
    """
    sales = session.read.parquet("data/sales_parquet")

    def hash_algo(order_id, bill_text):
        ret = bill_text.encode("utf-8")
        if int(order_id) % 2 == 0:
            cnt_A = bill_text.count("A")
            for _c in range(cnt_A):
                ret = hashlib.md5(ret).hexdigest().encode("utf-8")
            ret = ret.decode("utf-8")
        else:
            ret = hashlib.sha256(ret).hexdigest()

        return ret

    hash_algo_udf = session.udf.register("hash_algo", hash_algo)

    sales.\
        withColumn("hashed_bill", hash_algo_udf(F.col("order_id"), F.col("bill_raw_text"))).\
        groupby("hashed_bill").\
        agg(F.count("*").alias("cnt")).where(F.col("cnt") > 1).\
        show()


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local") \
        .config("spark.sql.autoBroadcastJoinThreshold", -1) \
        .config("spark.executor.memory", "3gb") \
        .appName("Exercise4") \
        .getOrCreate()

    main(spark)
