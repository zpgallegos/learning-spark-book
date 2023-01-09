from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as f

data = [
    [
        1,
        "Jules",
        "Damji",
        "https://tinyurl.1",
        "1/4/2016",
        4535,
        ["twitter", "LinkedIn"],
    ],
    [
        2,
        "Brooke",
        "Wenig",
        "https://tinyurl.2",
        "5/5/2018",
        8908,
        ["twitter", "LinkedIn"],
    ],
    [
        3,
        "Denny",
        "Lee",
        "https://tinyurl.3",
        "6/7/2019",
        7659,
        ["web", "twitter", "FB", "LinkedIn"],
    ],
    [4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
    [
        5,
        "Matei",
        "Zaharia",
        "https://tinyurl.5",
        "5/14/2014",
        40578,
        ["web", "twitter", "FB", "LinkedIn"],
    ],
    [
        6,
        "Reynold",
        "Xin",
        "https://tinyurl.6",
        "3/2/2015",
        25568,
        ["twitter", "LinkedIn"],
    ],
]

schema = (
    "`ID` INT,"
    "`First` STRING,"
    "`Last` STRING,"
    "`Url` STRING,"
    "`Published` STRING,"
    "`Hits` INT,"
    "`Campaigns` ARRAY<STRING>"
)

if __name__ == "__main__":

    spark = SparkSession.builder.appName("schema-ex").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(data, schema)
    print(df.printSchema())
    df.show(truncate=False)

    df.columns

    df.select(f.expr("Hits * 2")).show(2)

    df.withColumn("Big Hitters", f.expr("Hits > 10000")).show()

    df.withColumn(
        "AuthorsId", (f.concat(f.expr("First"), f.expr("Last"), f.expr("ID")))
    ).select("AuthorsId").show()

    row = Row(
        6,
        "Reynold",
        "Xin",
        "https://tinyurl.6",
        "3/2/2015",
        25568,
        ["twitter", "LinkedIn"],
    )
