from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, datediff, col, year, count

if __name__ == '__main__':
    # creating spark session object
    spark = SparkSession.builder. \
        appName("Task1"). \
        config("spark.sql.warehouse.dir", "hdfs:///apps/spark/warehouse"). \
        enableHiveSupport(). \
        getOrCreate()
    # Spark Context
    sc = spark.sparkContext

    # EXTRACTION
    # creating DataFrame loaded from kafka
    hotels = spark.read.format('kafka'). \
        option("kafka.bootstrap.servers", "sandbox-hdf.hortonworks.com:6667"). \
        option("subscribe", "ehhk"). \
        option("startingOffsets", "earliest"). \
        load()

    # extracting values from kafka topic and converting it to a dataframe
    clean_hotels_json = [row["value"] for row in hotels.selectExpr("CAST(value AS STRING)").collect()]
    clean_hotels_df = spark.read.json(sc.parallelize(clean_hotels_json))

    # creating external table over Hive to extract data with Avro format from HDFS
    spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS spark_expedia_hdfs
      ROW FORMAT SERDE
      'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
      STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
      OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
      LOCATION 'hdfs:///expedia/'
    TBLPROPERTIES (
      'avro.schema.literal'='{
      "type" : "record",
      "name" : "topLevelRecord",
      "fields" : [ {
        "name" : "id",
        "type" : [ "long", "null" ]
      }, {
        "name" : "date_time",
        "type" : [ "string", "null" ]
      }, {
        "name" : "site_name",
        "type" : [ "int", "null" ]
      }, {
        "name" : "posa_continent",
        "type" : [ "int", "null" ]
      }, {
        "name" : "user_location_country",
        "type" : [ "int", "null" ]
      }, {
        "name" : "user_location_region",
        "type" : [ "int", "null" ]
      }, {
        "name" : "user_location_city",
        "type" : [ "int", "null" ]
      }, {
        "name" : "orig_destination_distance",
        "type" : [ "double", "null" ]
      }, {
        "name" : "user_id",
        "type" : [ "int", "null" ]
      }, {
        "name" : "is_mobile",
        "type" : [ "int", "null" ]
      }, {
        "name" : "is_package",
        "type" : [ "int", "null" ]
      }, {
        "name" : "channel",
        "type" : [ "int", "null" ]
      }, {
        "name" : "srch_ci",
        "type" : [ "string", "null" ]
      }, {
        "name" : "srch_co",
        "type" : [ "string", "null" ]
      }, {
        "name" : "srch_adults_cnt",
        "type" : [ "int", "null" ]
      }, {
        "name" : "srch_children_cnt",
        "type" : [ "int", "null" ]
      }, {
        "name" : "srch_rm_cnt",
        "type" : [ "int", "null" ]
      }, {
        "name" : "srch_destination_id",
        "type" : [ "int", "null" ]
      }, {
        "name" : "srch_destination_type_id",
        "type" : [ "int", "null" ]
      }, {
        "name" : "hotel_id",
        "type" : [ "long", "null" ]
      } ]
    }
    ')""")
    # creating dataframe with booking data exported through HDFS
    expedia_df = spark.sql("SELECT * FROM spark_expedia_hdfs")

    # creating window object for idle_days calculation
    window_spec = Window.partitionBy("hotel_id").orderBy("srch_ci")

    # calculating idle days, appending it to the current booking data and saving it to the new DataFrame
    expedia_df_with_idle_days = expedia_df. \
        withColumn("idle_days", datediff(expedia_df.srch_ci, lag("srch_ci", 1).over(window_spec)))

    # VALIDATION
    # filtering invalid hotel ids
    invalid_hotel_ids = expedia_df_with_idle_days. \
        filter((col("idle_days") >= 2) & (col("idle_days") < 30)). \
        select("hotel_id"). \
        distinct(). \
        collect()

    # creating broadcast variable to access list of invalid hotel ids on each node.
    invalid_hotel_ids_br = sc.broadcast([row["hotel_id"] for row in invalid_hotel_ids])

    # booking data cleaned from invalid hotels data
    clean_expedia_df_with_idle_days = expedia_df_with_idle_days. \
        filter(~col("hotel_id").isin(invalid_hotel_ids_br.value))

    # LOAD/DISPLAY
    # display invalid hotels data
    clean_hotels_df.filter(col("Id").isin(invalid_hotel_ids_br.value)).show()

    # create window spec to count bookings grouped by hotel's country
    window_on_country = Window.partitionBy("Country")
    expedia_df_with_idle_days. \
        join(clean_hotels_df, clean_hotels_df.Id == expedia_df_with_idle_days.hotel_id). \
        withColumn("bookings_by_country", count(expedia_df_with_idle_days.id).over(window_on_country)). \
        select(col("Country"), col("bookings_by_country")). \
        distinct(). \
        show()

    # create window spec to count bookings grouped by hotel's city
    window_on_city = Window.partitionBy("City")
    expedia_df_with_idle_days. \
        join(clean_hotels_df, clean_hotels_df.Id == expedia_df_with_idle_days.hotel_id). \
        withColumn("bookings_by_city", count(expedia_df_with_idle_days.id).over(window_on_city)). \
        select(col("Country"), col("City"), col("bookings_by_city")).distinct().show()

    # save cleaned booking data partitioned by year of check_in
    expedia_df_with_idle_days. \
        withColumn("year", year(expedia_df_with_idle_days.srch_ci)). \
        write. \
        partitionBy("year"). \
        format("parquet"). \
        save("hdfs:///tmp/spark/clean_expedia_df2")
