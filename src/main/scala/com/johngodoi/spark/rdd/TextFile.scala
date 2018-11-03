package com.johngodoi.spark.rdd

/**
  * Created by jgodoi on 5/11/2017.
  */
object TextFile extends App {
  val conf = new SparkConf().setAppName("initial").setMaster("local")
  val sc = new SparkContext(conf)
  val file = sc.textFile("build.sbt")
  println(file.count())
  sc.stop()
}
