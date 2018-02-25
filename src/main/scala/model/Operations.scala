package model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime


class Operations {
  val sc = new SparkContext("local", "Operations")
  val customer = sc.textFile(CUSTOMER_FILE).map { x => x.split('#') }
  val sales = sc.textFile(SALES_FILE).map { x => x.split('#') }
}
