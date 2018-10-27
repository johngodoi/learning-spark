package com.johngodoi.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Take extends App {
  val conf = new SparkConf().setAppName("initial").setMaster("local")
  val sc = new SparkContext(conf)

  val nums: RDD[Int] = sc.parallelize(List(0,1,2,3,4,5,6,7,8,9).reverse)

  println(nums.take(5).toList)
}
