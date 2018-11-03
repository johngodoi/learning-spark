package com.johngodoi.spark.utils

object Utils {
  def isNumeric(value: String) = {
    value.matches("\\d+")
  }
}
