package com.johngodoi.spark

import org.apache.spark.{SparkConf, SparkContext}

object GroupBy extends App {

  val conf = new SparkConf().setAppName("initial").setMaster("local")
  val sc = new SparkContext(conf)
  sc.parallelize(List(1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,1,2,3,1,2))
    .groupBy(n => n)
    .mapValues(v => v.size)
    .foreach(p => println(p))
  sc.stop()

}
