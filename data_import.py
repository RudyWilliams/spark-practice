from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType

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

fifa_schema = (
    StructType()
    .add("Name", StringType(), True)
    .add("Age", IntegerType(), True)
    .add("Nationality", StringType(), True)
    .add("Overall", IntegerType(), True)
    .add("Potential", IntegerType(), True)
    .add("Club", StringType(), True)
    .add("Value", StringType(), True)
    .add("Wage", StringType(), True)
    .add("Preferred Foot", StringType(), True)
    .add("International Reputation", IntegerType(), True)
    .add("Weak Foot", IntegerType(), True)
    .add("Skill Moves", IntegerType(), True)
    .add("Work Rate", StringType(), True)
    .add("Body Type", StringType(), True)
    .add("Position", StringType(), True)
    .add("Jersey Number", IntegerType(), True)
    .add("Joined", StringType(), True)
    .add("Contract Valid Until", StringType(), True)
    .add("Height", StringType(), True)
    .add("Weight", StringType(), True)
    .add("Crossing", IntegerType(), True)
    .add("Finishing", IntegerType(), True)
    .add("Marking", IntegerType(), True)
)

# fifa_schema = StructType(
#     [
#         StructField("ID", IntegerType(), True),
#         StructField("Name", StringType(), True),
#         StructField("Age", IntegerType(), True),
#         StructField("Nationality", StringType(), True),
#         StructField("Overall", IntegerType(), True),
#         StructField("Potential", IntegerType(), True),
#         StructField("Club", StringType(), True),
#         StructField("Value", StringType(), True),
#         StructField("Wage", StringType(), True),
#         StructField("Preferred Foot", StringType(), True),
#         StructField("International Reputation", IntegerType(), True),
#         StructField("Weak Foot", IntegerType(), True),
#         StructField("Skill Moves", IntegerType(), True),
#         StructField("Work Rate", StringType(), True),
#         StructField("Body Type", StringType(), True),
#         StructField("Position", StringType(), True),
#         StructField("Jersey Number", IntegerType(), True),
#         StructField("Joined", DateType(), True),
#         StructField("Contract Valid Until", IntegerType(), True),
#         StructField("Height", StringType(), True),
#         StructField("Weight", StringType(), True),
#         StructField("Crossing", IntegerType(), True),
#         StructField("Finishing", IntegerType(), True),
#         StructField("Marking", IntegerType(), True),
#     ]
# )

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
