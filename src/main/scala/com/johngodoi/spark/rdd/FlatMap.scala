package com.johngodoi.spark.rdd

object FlatMap extends App {
  val conf = new SparkConf().setAppName("initial").setMaster("local")
  val sc = new SparkContext(conf)
  val nums = sc.parallelize(List(List(1,2),List(3,4)))
  val squared = nums.flatMap(x=>x).map(x=>x*x)
  println(squared.sum())

  sc.stop()

}
