package com.johngodoi.spark.iran_tweets

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Download data from: https://about.twitter.com/en_us/values/elections-integrity.html#data
  * Not working on my machine
  */
object IranTweetsWordCount extends App {
  private val spark = SparkSession.builder().appName("iran_tweets").master("local[*]").getOrCreate()
  import spark.implicits._
  private val dataFrame: DataFrame = spark.read.option("header","true").csv("./src/main/resources/ira_tweets/ira_tweets_csv_hashed.csv")
  dataFrame.select("tweet_text").flatMap(row => row.getString(0).split(" "))
    .rdd.groupBy(word=> word)
    .mapValues(value => value.size).filter(pair => pair._2>1000).foreach(println(_))
  spark.stop()
}
