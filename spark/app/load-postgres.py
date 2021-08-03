import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, to_timestamp
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import regexp_extract, col,expr, to_timestamp,weekofyear,year, hour,udf
from pyspark.sql.types import DecimalType,StringType
from pyspark.sql.functions import udf
from pyspark.ml.feature import Bucketizer


# Create spark session
spark = (SparkSession
    .builder
    .getOrCreate()
)

####################################
# Parameters
####################################
# movies_file = sys.argv[1]
trips_file = sys.argv[1]
postgres_db = sys.argv[2]
postgres_user = sys.argv[3]
postgres_pwd = sys.argv[4]

####################################
# Read CSV Data
####################################
print("######################################")
print("READING CSV FILES")
print("######################################")

df_trips_csv = (
    spark.read
    .format("csv")
    .option("header", True)
    .load(trips_file)
)

result_origin_x = df_trips_csv.withColumn('origin_x', regexp_extract(col('origin_coord'), '([0-9]+.[0-9]+) ([0-9]+.[0-9]+)', 1))
output_origin_x_df = result_origin_x.withColumn("origin_x",result_origin_x["origin_x"].cast(DecimalType(4,1)))

result_origin_y = output_origin_x_df.withColumn('origin_y', regexp_extract(col('origin_coord'), '([0-9]+.[0-9]+) ([0-9]+.[0-9]+)', 2))
output_origin_y_df = result_origin_y.withColumn("origin_y",result_origin_y["origin_y"].cast(DecimalType(4,1)))

result_destination_x = output_origin_y_df.withColumn('dest_x', regexp_extract(col('destination_coord'), '([0-9]+.[0-9]+) ([0-9]+.[0-9]+)', 1))
output_destination_x_df = result_destination_x.withColumn("dest_x",result_destination_x["dest_x"].cast(DecimalType(4,1)))

result_destination_y = output_destination_x_df.withColumn('dest_y', regexp_extract(col('destination_coord'), '([0-9]+.[0-9]+) ([0-9]+.[0-9]+)', 2))
output_destination_y_df = result_destination_y.withColumn("dest_y",result_destination_y["dest_y"].cast(DecimalType(4,1)))


output_date_df = output_destination_y_df.withColumn("datetime_only", to_timestamp(col("datetime"),'yyyy-MM-dd HH:mm:ss'))

output_week_df = output_date_df.withColumn('week',weekofyear(col("datetime_only")))
output_year_df = output_week_df.withColumn('year',year(col("datetime_only")))
output_hour_df = output_year_df.withColumn('hour',hour(col("datetime_only")))



bucketizer = Bucketizer(splits=[ 0,4,8, 12, 16, 20, 24 ],inputCol="hour", outputCol="buckets")
df_buck = bucketizer.setHandleInvalid("keep").transform(output_hour_df)

t = {0.0:"Late Night", 1.0: "Early Morning", 2.0:"Morning", 3.0: "Noon", 4.0: "Eve", 5.0: "Noon"}
udf_foo = udf(lambda x: t[x], StringType())
output_session_df = df_buck.withColumn("hour_bucket", udf_foo("buckets"))
output_session_droped_df = output_session_df.drop('buckets')
# output_destination_y_df.show()
# df_ratings_csv = (
#     spark.read
#     .format("csv")
#     .option("header", True)
#     .load(ratings_file)
#     .withColumnRenamed("timestamp","timestamp_epoch")
# )

# # Convert epoch to timestamp and rating to DoubleType
# df_ratings_csv_fmt = (
#     df_ratings_csv
#     .withColumn('rating', col("rating").cast(DoubleType()))
#     .withColumn('timestamp', to_timestamp(from_unixtime(col("timestamp_epoch"))))
# )

####################################
# Load data to Postgres
####################################
print("######################################")
print("LOADING POSTGRES TABLES")
print("######################################")

(
    output_session_droped_df.write
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", "public.trip_transformed")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .mode("overwrite")
    .save()
)

# (
#      df_ratings_csv_fmt
#      .select([c for c in df_ratings_csv_fmt.columns if c != "timestamp_epoch"])
#      .write
#      .format("jdbc")
#      .option("url", postgres_db)
#      .option("dbtable", "public.ratings")
#      .option("user", postgres_user)
#      .option("password", postgres_pwd)
#      .mode("overwrite")
#      .save()
# )