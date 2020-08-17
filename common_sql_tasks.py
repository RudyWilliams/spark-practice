"""
Covers filtering, sorting, distinct

Some will come from book (Spark the Definitive Guide) and other techniques
may come from https://sparkbyexamples.com/pyspark/pyspark-where-filter/
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, lit, col, column
from data_import import (
    read_csv_into_pyspark,
    movies_schema,
    ratings_schema,
    fifa_schema,
)

spark = SparkSession.builder.getOrCreate()

movies_df = read_csv_into_pyspark(
    spark_sess=spark,
    schema=movies_schema,
    path="data/movies.txt",
    header="false",
    sep="*",
)

ratings_df = read_csv_into_pyspark(
    spark_sess=spark,
    schema=ratings_schema,
    path="data/ratings.txt",
    header="false",
    sep="*",
)

fifa_df = read_csv_into_pyspark(
    spark_sess=spark,
    schema=fifa_schema,
    path="data/fifa_data.csv",
    header="true",
    sep=",",
)

## filtering rows (book)
all_fives = ratings_df.where(col("rating") == 5)
# print(all_fives.show())

## multiple filters
## can put multiple in same expression but Spark performs all at same time
## so can just chain .where() or .filters (only for AND)
greater_than_3_id_1 = ratings_df.where("rating > 3").where("user_id == 1")
# print(greater_than_3_id_1.show())
## OR
gt3_id1 = ratings_df.where((col("rating") > 3) & (col("user_id") == 1))
# print(gt3_id1.show())

## FIFA Data SQL Type Uses
# productive offensive players
good_finishing_or_crossing = fifa_df.select("Name", "Finishing", "Crossing").where(
    (col("Finishing") >= 80) | (col("Crossing") >= 80)
)
# print(good_finishing_or_crossing.show())

# get distinct countries and sort alphebetically
countries = fifa_df.select("Nationality").distinct().orderBy("Nationality")
# print(countries.show())

# get count of number of distinct countires
country_count = fifa_df.select("Nationality").distinct().count()
# print(country_count) # returns an int so no show needed (count is the action)
# print(countries.count()) # another way to get it
# --- OR ---
country_count2 = fifa_df.selectExpr("count(distinct(Nationality)) as num_countries")
print(country_count2.show())  # returns df so show (action) needed
