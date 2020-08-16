from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, lit, col, column
from data_import import read_csv_into_pyspark, movies_schema, ratings_schema

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

## select vs selectExpr--------------------------------------------------------
## allow for doing the df equivalent of SQL queries
## select: use method for columns or expressions
## selectExpr: use method when dealing with expressions in strings

titles = movies_df.select("title")
titles_genres = movies_df.select("title", "genres")

using_expr_simple = movies_df.select(expr("title as movie_title"))

## using selectExpr

movie_starts_with_T_selectExpr = movies_df.selectExpr(
    "title", "genres", '(title LIKE "T%") as starts_with_T'
)

movie_id_one_selectExpr = movies_df.selectExpr("*", "(movie_id = 1) as id_one")
# instead of
movie_id_one_select = movies_df.select(expr("*"), expr("(movie_id = 1) as id_one"))

# can perform aggregations on entire dataframe
agg_with_selectExpr = movies_df.selectExpr(
    "count(title)", "avg(movie_id)", "count(movie_id)", "count(distinct(movie_id))"
)


## creating new column---------------------------------------------------------
# withColumn
with_column = ratings_df.withColumn("number_5", lit(5)).withColumn("letter_a", lit("A"))
# print(with_column.show())

top_ratings_bool = ratings_df.withColumn("four_plus_stars", expr("rating >= 4"))
# print(top_ratings_bool.show())


## removing columns------------------------------------------------------------
dropped_top_ratings_bool = top_ratings_bool.drop(
    "four_plus_stars"
)  # just original data but for illustrative purposes
# print(dropped_top_ratings_bool.show())
dropped_multiple_columns = top_ratings_bool.drop("four_plus_stars", "timestamp")
# print(dropped_multiple_columns.show())


## changing a column's type (casting)------------------------------------------
int_now_long = ratings_df.withColumn("rating", expr("rating").cast("long"))
# print(int_now_long.show())
# OR
int_now_long = ratings_df.withColumn("rating", col("rating").cast("long"))
# print(int_now_long.show())
# OR
int_now_long = ratings_df.withColumn("rating", column("rating").cast("long"))
# print(int_now_long.show())

# ***columns can be referred to by: expr(), col(), or column()***

