package com.whishworks.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

/**
  * @author: Gaurav Bhardwaj
  * This is a sample spark job reading data from csv file and doing simple transformation.
  */
object EmployeeJob {
  def main(args: Array[String]): Unit = {
    if (args.isEmpty || args(0).isEmpty){
      println("Usage: com.whishworks.spark.EmployeeJob <path of the data>")
      sys.exit(0)
    }

    val spark = SparkSession.builder().appName("Employee Salary Statistics").enableHiveSupport().getOrCreate()

    import spark.implicits._

    val data = spark.read.option("header", "true").csv(args(0))

    val salaryStats = data.
      withColumn("Typical Hours", $"Typical Hours".cast(IntegerType)).
      withColumn("Annual Salary", $"Annual Salary".cast(DoubleType)).
      withColumn("Hourly Rate", $"Hourly Rate".cast(DoubleType)).
      withColumn("Salary", when($"Salary or Hourly" === "Salary", $"Annual Salary").otherwise($"Hourly Rate" * $"Typical Hours" * 52)).
      drop("Name", "Job Titles", "Full or Part-Time", "Salary or Hourly", "Typical Hours", "Annual Salary", "Hourly Rate").
      groupBy($"Department").
      agg(round(avg($"Salary"), 2).alias("Avg"),
        round(min($"Salary"), 2).alias("Min"),
        round(max($"Salary"), 2).alias("Max"))

    salaryStats.collect().foreach(row => println(s"${row(0)} department has average salary: ${row(1)}, minimum salary: ${row(2)} and max salary: ${row(3)}"))

    spark.stop()
  }
}
