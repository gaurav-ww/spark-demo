package com.whishworks.spark

import org.apache.spark.{SparkConf, SparkContext}

object SampleJob {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Sample App")
    val sc = new SparkContext(conf)
    val dummyData = 10 to 0 by -1
    val rdd = sc.parallelize(dummyData, 1)
    val data = rdd.collect()
    data.foreach(println)
    println("Everything is working fine!")
    sc.stop()
  }

}
