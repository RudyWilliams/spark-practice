"""
Covers filtering (.where()), sortBy, distinct along with aggregations and groupBy

Some will come from book (Spark the Definitive Guide) and other techniques
may come from https://sparkbyexamples.com/pyspark/pyspark-where-filter/
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, lit, col, column, countDistinct
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

# in order to get the money and special chars to read in,
# I had to change the CSV to be saved as UTF csv
fifa_df = read_csv_into_pyspark(
    spark_sess=spark,
    schema=fifa_schema,
    path="data/fifa_data.csv",
    header="true",
    sep=",",
)


## filtering rows (book)-------------------------------------------------------
all_fives = ratings_df.where(col("rating") == 5)
# all_fives.show()

## multiple filters
## can put multiple in same expression but Spark performs all at same time
## so can just chain .where() or .filters (only for AND)
greater_than_3_id_1 = ratings_df.where("rating > 3").where("user_id == 1")
# greater_than_3_id_1.show()
## OR
gt3_id1 = ratings_df.where((col("rating") > 3) & (col("user_id") == 1))
# gt3_id1.show()

## FIFA Data
# productive offensive players-------------------------------------------------
good_finishing_or_crossing = fifa_df.select("Name", "Finishing", "Crossing").where(
    (col("Finishing") >= 80) | (col("Crossing") >= 80)
)
# good_finishing_or_crossing.show()

# get distinct countries and sort alphebetically-------------------------------
countries = fifa_df.select("Nationality").distinct().orderBy("Nationality")
# countries.show()

# get count of number of distinct countires------------------------------------
country_count = fifa_df.select("Nationality").distinct().count()
# print(country_count) # returns an int so no show needed (count is the action)
# countries.count() # another way to get it
# --- OR ---
country_count2 = fifa_df.selectExpr("count(distinct(Nationality)) as num_countries")
# country_count2.show()  # returns df so show (action) needed
# --- OR ---
country_count3 = fifa_df.select(countDistinct("Nationality"))
# country_count3.show()


## Aggregations
from pyspark.sql.functions import min, max, sum, count, avg, expr, asc, desc

# show lowest and largest overall
lowest_overall = fifa_df.select(min("Overall"), max("Overall"))
# lowest_overall.show()

avg_overall = fifa_df.select(
    avg("Overall").alias("way1"), expr("mean(Overall)").alias("way2")
)
# avg_overall.show()


## GROUPBY
nation_avg_overall = (
    fifa_df.groupBy("Nationality").avg("Overall").orderBy(desc("avg(Overall)"))
)
# --- OR ---
nation_avg_overall_2 = (
    fifa_df.select("Nationality", "Overall")
    .groupBy("Nationality")
    .avg()
    .orderBy(col("avg(Overall)").desc())
)
# nation_avg_overall_2.where(col("Nationality") == "Germany").show()

# could be thrown off by countries that only have a few number of players in the data
# remove the countries that don't have more than 20 players in the data
result2 = (
    fifa_df.select("Nationality", "Overall")
    .groupBy("Nationality")
    .agg(
        count("Overall"), avg("Overall")
    )  # << for how to do multiple aggregate functions
    .where(col("count(Overall)") > 20)
    .orderBy(col("avg(Overall)").desc())
)
# result2.where(col("Nationality") == "Germany").show()

# show the data for Germany, Peru, United States, and England
isin_example = result2.select("*").where(
    col("Nationality").isin(["Germany", "Peru", "United States", "England"])
)
isin_example.show()
