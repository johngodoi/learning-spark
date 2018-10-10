package com.johngodoi.spark.excel

import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadExcel extends App {
  private val spark = SparkSession.builder().appName("read_excel").master("local").getOrCreate()

  def readExcel(file: String): DataFrame = spark.read
    .format("com.crealytics.spark.excel")
    .option("location", file)
    .option("useHeader", "true")
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "false")
    .option("addColorColumns", "False")
    .load()

  val data = readExcel("path.xlsx")
  data.show(false)
  spark.stop()
}
