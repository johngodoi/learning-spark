package com.johngodoi.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jgodoi on 5/11/2017.
  */
object TextFile extends App {
  val conf = new SparkConf().setAppName("initial").setMaster("local")
  val sc = new SparkContext(conf)
  sc.setLogLevel("OFF")
  val file = sc.textFile("build.sbt")
  println(file.filter(x => !x.isEmpty).filter(x => x.contains(":=")).map(x=>x.split(":=")(1).trim).collect().mkString(","))
  sc.stop()
}
