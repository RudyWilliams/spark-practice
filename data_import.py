from pyspark.sql.types import StructField, StructType, IntegerType, StringType

## one way to read csv into spark dataframe

# movies_df = (
#     spark.read.format("csv")
#     .schema(movies_schema)
#     .option("header", "false")
#     .option("sep", "*")
#     .option("path", "data/movies.txt")
#     .load()
# )

## converted to function
def read_csv_into_pyspark(spark_sess, schema, path, header, sep):
    return (
        spark_sess.read.format("csv")
        .schema(schema)
        .option("header", header)
        .option("sep", sep)
        .option("path", path)
        .load()
    )


# define schema for movies and ratings dfs
movies_schema = StructType(
    [
        StructField(name="movie_id", dataType=IntegerType(), nullable=False),
        StructField(name="title", dataType=StringType(), nullable=True),
        StructField(name="genres", dataType=StringType(), nullable=True),
    ]
)

ratings_schema = StructType(
    [
        StructField(name="user_id", dataType=IntegerType(), nullable=False),
        StructField(name="movie_id", dataType=IntegerType(), nullable=False),
        StructField(name="rating", dataType=IntegerType(), nullable=True),
        StructField(name="timestamp", dataType=IntegerType(), nullable=True),
    ]
)

if __name__ == "__main__":
    from pyspark.sql import SparkSession

    # from pyspark import SparkContext
    # sc = SparkContext()
    # spark = SparkSession(sc)

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
    print(movies_df.show())
    print(ratings_df.show())
