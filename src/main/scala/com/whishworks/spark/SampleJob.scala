package com.whishworks.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * @author: Gaurav Bhardwaj
  * This is a sample spark job which create small sample data and pushes it to hive table.
  */
object SampleJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Sample App").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val currentTime = format.format(new java.util.Date())
    val dummyData = (1 to 10).map(Row(_, currentTime))

    val rdd = sc.parallelize(dummyData, 1)


    spark.sql("CREATE TABLE IF NOT EXISTS demo(id INT, cur_time STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")

    val schema = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("cur_time", StringType, false)))

    val df = spark.createDataFrame(rdd, schema)
    df.write.mode(SaveMode.Overwrite).format("hive").saveAsTable("demo")

    println("Everything is working fine!")
    spark.stop()
  }

}
