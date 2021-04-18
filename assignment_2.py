from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, LongType, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import datediff, col, when, current_timestamp, greatest, sum

# SCHEMA FOR THE RESULT STREAM
result_schema = (StructType().
                 add("hotel_id", LongType()).
                 add("erroneous_data_cnt", LongType()).
                 add("short_stay_cnt", LongType()).
                 add("standard_stay_cnt", LongType()).
                 add("standard_extended_stay_cnt", LongType()).
                 add("long_stay_cnt", LongType()).
                 add("batch_timestamp", TimestampType()).
                 add("most_popular_stay_type", StringType())
                 )

# SCHEMA FOR BOOKING DATA
expedia_schema = (StructType().
                  add("id", LongType(), True).
                  add("date_time", StringType(), True).
                  add("site_name", IntegerType(), True).
                  add("posa_continent", IntegerType(), True).
                  add("user_location_country", IntegerType(), True).
                  add("user_location_region", IntegerType(), True).
                  add("user_location_city", IntegerType(), True).
                  add("orig_destination_distance", DoubleType(), True).
                  add("user_id", IntegerType(), True).
                  add("is_mobile", IntegerType(), True).
                  add("is_package", IntegerType(), True).
                  add("channel", IntegerType(), True).
                  add("srch_ci", StringType(), True).
                  add("srch_co", StringType(), True).
                  add("srch_adults_cnt", IntegerType(), True).
                  add("srch_children_cnt", IntegerType(), True).
                  add("srch_rm_cnt", IntegerType(), True).
                  add("srch_destination_id", IntegerType(), True).
                  add("srch_destination_type_id", IntegerType(), True).
                  add("hotel_id", LongType(), True).
                  add("idle_days", IntegerType(), True))


def assignment_transformation(expedia_df,  hotels_weather_df):
    # enriching booking data with avg temp on the srch_ci and duration of stay
    expedia_enriched = (expedia_df.
                        join(hotels_weather_df,
                             (expedia_df.hotel_id == hotels_weather_df.id) &
                             (expedia_df.srch_ci == hotels_weather_df.wthr_date)).
                        select(expedia_df["*"], hotels_weather_df.avg_c).
                        withColumn("duration_of_stay", datediff(col("srch_co"), col("srch_ci")))
                        )
    # enriching expedia data with stay types for further summing
    stay_data = (expedia_enriched.
                 withColumn("short_stay", when(col("duration_of_stay") == 1, 1).otherwise(0)).
                 withColumn("erroneous_data", when((col("duration_of_stay") <= 0) |
                                                   (col("duration_of_stay") > 30) |
                                                   (col("duration_of_stay").isNull()), 1).otherwise(0)).
                 withColumn("standard_stay",
                            when((col("duration_of_stay") >= 2) & (col("duration_of_stay") < 7), 1).otherwise(0)).
                 withColumn("standard_extended_stay",
                            when((col("duration_of_stay") >= 8) & (col("duration_of_stay") < 14), 1).otherwise(0)).
                 withColumn("long_stay",
                            when((col("duration_of_stay") >= 15) & (col("duration_of_stay") < 30), 1).otherwise(0)).
                 withColumn("batch_timestamp", current_timestamp())
                 )
    # if dataframe is streaming for aggregation we need to define a watermark period.
    stay_data = stay_data.withWatermark("batch_timestamp", "1 minute") if stay_data.isStreaming else stay_data

    # grouping and calculating stay types for each hotel
    cnt = (stay_data.
           groupBy("hotel_id", "batch_timestamp").
           agg(sum("short_stay").alias("short_stay_cnt"),
               sum("erroneous_data").alias("erroneous_data_cnt"),
               sum("standard_stay").alias("standard_stay_cnt"),
               sum("standard_extended_stay").alias("standard_extended_stay_cnt"),
               sum("long_stay").alias("long_stay_cnt")
               )
           )

    # calculating most popular stay type for each hotel
    return (cnt.
            withColumn("popular_stay_cnt", greatest("erroneous_data_cnt",
                                                    "short_stay_cnt",
                                                    "standard_stay_cnt",
                                                    "standard_extended_stay_cnt",
                                                    "long_stay_cnt")).
            withColumn("most_popular_stay_type",
                       when(col("popular_stay_cnt") == cnt["erroneous_data_cnt"], "Erroneous data").
                       when(col("popular_stay_cnt") == cnt["short_stay_cnt"], "Short stay").
                       when(col("popular_stay_cnt") == cnt["standard_stay_cnt"], "Standard stay").
                       when(col("popular_stay_cnt") == cnt["standard_extended_stay_cnt"], "Standard extended stay").
                       when(col("popular_stay_cnt") == cnt["long_stay_cnt"], "Long stay")).
            select(cnt["*"], col("most_popular_stay_type"))
            )


if __name__ == '__main__':
    # creating spark session object
    spark = (SparkSession.builder.
             appName("Task2").
             getOrCreate())
    # Spark Context
    sc = spark.sparkContext

    # EXTRACTION
    # BATCH
    # creating DataFrame loaded from kafka
    hotels_weather = (spark.read.format('kafka').
                      option("kafka.bootstrap.servers", "sandbox-hdf.hortonworks.com:6667").
                      option("subscribe", "ehhwk").
                      option("startingOffsets", "earliest").
                      load())

    hotels_weather_json = [row["value"] for row in hotels_weather.selectExpr("CAST(value AS STRING)").collect()]
    hotels_weather_df = spark.read.json(sc.parallelize(hotels_weather_json))
    hotels_weather_df = hotels_weather_df.filter(col("avg_c") > 0).select("id", "wthr_date", "avg_c")
    hotels_weather_df.cache()

    # expedia_df_2016 = (spark.
    #                    read.
    #                    schema(expedia_schema).
    #                    parquet("hdfs:///tmp/spark/clean_expedia_df/year=2016"))
    # result_df_2016 = assignment_transformation(expedia_df_2016, hotels_weather_df)
    # result_df_2016.write.format('parquet').save("hdfs:///tmp/spark/tmp_initial_data")

    # STREAM
    # result_stream_2016 = spark.readStream.schema(result_schema).parquet("hdfs:///tmp/spark/tmp_initial_data")

    expedia_stream_2017 = (spark.
                           readStream.
                           option("maxFilesPerTrigger", 1).
                           schema(expedia_schema).
                           parquet("hdfs:///tmp/spark/clean_expedia_df/year=2017")
                           )
    result_stream_2017 = assignment_transformation(expedia_stream_2017, hotels_weather_df)

    # WRITING
    # adjusted to writing to elasticsearch
    # query_2016 = (result_stream_2016.
    #               writeStream.
    #               outputMode("append").
    #               option("checkpointLocation", "/tmp/spark/checkpoint/elastic_2016").
    #               format("org.elasticsearch.spark.sql").
    #               option("es.resource", "spark_docs_2016").
    #               option("es.net.http.auth.user", "elastic").
    #               option("es.net.http.auth.pass", "changeme").
    #               option("es.index.auto.create", "true").
    #               option("es.nodes", "elasticsearch").
    #               option("es.port", "9200").
    #               start())

    query_2017 = (result_stream_2017.
                  writeStream.
                  outputMode("append").
                  option("checkpointLocation", "/tmp/spark/checkpoint/elastic_kek").
                  format("org.elasticsearch.spark.sql").
                  option("es.resource", "spark_docs").
                  option("es.net.http.auth.user", "elastic").
                  option("es.net.http.auth.pass", "changeme").
                  option("es.index.auto.create", "true").
                  option("es.nodes", "elasticsearch").
                  option("es.port", "9200").
                  start())

    spark.streams.awaitAnyTermination()
    hotels_weather_df.unpersist()
