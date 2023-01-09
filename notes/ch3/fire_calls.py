from pyspark.sql.types import *
from pyspark.sql import functions as f

fire_schema = StructType(
    [
        StructField("CallNumber", IntegerType(), True),
        StructField("UnitID", StringType(), True),
        StructField("IncidentNumber", IntegerType(), True),
        StructField("CallType", StringType(), True),
        StructField("CallDate", StringType(), True),
        StructField("WatchDate", StringType(), True),
        StructField("CallFinalDisposition", StringType(), True),
        StructField("AvailableDtTm", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Zipcode", IntegerType(), True),
        StructField("Battalion", StringType(), True),
        StructField("StationArea", StringType(), True),
        StructField("Box", StringType(), True),
        StructField("OriginalPriority", StringType(), True),
        StructField("Priority", StringType(), True),
        StructField("FinalPriority", IntegerType(), True),
        StructField("ALSUnit", BooleanType(), True),
        StructField("CallTypeGroup", StringType(), True),
        StructField("NumAlarms", IntegerType(), True),
        StructField("UnitType", StringType(), True),
        StructField("UnitSequenceInCallDispatch", IntegerType(), True),
        StructField("FirePreventionDistrict", StringType(), True),
        StructField("SupervisorDistrict", StringType(), True),
        StructField("Neighborhood", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("RowID", StringType(), True),
        StructField("Delay", FloatType(), True),
    ]
)

sf_fire_file = "../../LearningSparkV2/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

# number of rows in fire_df
fire_df.count()

# fire_df.write.format("parquet").save("fire_df.parquet")

a = fire_df.select("IncidentNumber", "AvailableDtTm", "CallType").where(
    f.col("CallType") != "Medical Incident"
)

b = (
    fire_df.select("CallType")
    .where(f.col("CallType").isNotNull())
    .agg(f.countDistinct("CallType").alias("DistinctCallTypes"))
)

# count by CallType
c = (
    fire_df.select("CallType")
    .where(f.col("CallType").isNotNull())
    .groupBy("CallType")
    .count()
)

# rename Delay to ResponseDelayedinMins
d = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
d.where(f.col("ResponseDelayedinMins") > 5).count()

# select minimum CallDate
fire_df.select(f.min("CallDate")).show()

# convert IncidentDate, OnWatchDate, and AvailableDtTm to timestamps
q = (
    fire_df.withColumn(
        "IncidentDate",
        f.to_timestamp(f.col("CallDate"), "MM/dd/yyyy"),
    )
    .drop("CallDate")
    .withColumn("OnWatchDate", f.to_timestamp(f.col("WatchDate"), "MM/dd/yyyy"))
    .drop("WatchDate")
    .withColumn(
        "AvailableDtTm", f.to_timestamp(f.col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")
    )
    .drop("AvailableDtTm")
)

q.select("CallType").where(f.col("CallType").isNotNull()).groupBy(
    "CallType"
).count().orderBy("count", ascending=False).show()

(
    q.select(
        f.sum("NumAlarms"),
        f.avg("Delay"),
        f.min("Delay"),
        f.max("Delay"),
    ).show()
)
