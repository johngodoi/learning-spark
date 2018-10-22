package com.johngodoi.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Math extends App {

  val conf = new SparkConf().setAppName("initial").setMaster("local")
  val sc = new SparkContext(conf)

  val nums: RDD[Int] = sc.parallelize(List(0,1,2,3,4,5,6,7,8,9)).cache()
  List(
    nums.reduce((x,y)=>x+y),
    nums.sum,
    nums.stats(),
    nums.stdev(),
    nums.variance(),
    nums.mean(),
    nums.max(),
    nums.min(),
    nums.count(),
    nums.sampleVariance(),
    nums.sampleStdev()
  ).foreach(println(_))

  sc.stop()
}

/*
foreach​
isEmpty
count​

reduce​
collect​

first​
take​

saveAsObjectFile
saveAsTextFile
*/
