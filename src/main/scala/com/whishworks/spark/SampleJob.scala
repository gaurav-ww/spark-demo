package com.whishworks.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object SampleJob {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Sample App")
    val session = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sc = session.sparkContext
    //val sc = new SparkContext(conf)
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val currentTime = format.format(new java.util.Date())
    val dummyData = (1 to 10).map(Row(_, currentTime))

    val rdd = sc.parallelize(dummyData, 1)


    session.sql("CREATE TABLE IF NOT EXISTS demo(id INT, cur_time STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")

    val schema = StructType(Seq(
      StructField("product", IntegerType, false),
      StructField("aldi_price", StringType, false)))

    val df = session.createDataFrame(rdd, schema)
    df.write.mode(SaveMode.Overwrite).format("hive").saveAsTable("demo")

    println("Everything is working fine!")
    sc.stop()
  }

}
