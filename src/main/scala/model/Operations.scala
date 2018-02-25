package model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
  * Functionality of the whole assignment is provided here. Joda-Time is used for Epoch Conversions to Normal Date-Time.
  */

class Operations {
  val sc = new SparkContext("local", "Operations")
  val customerRawData = sc.textFile(CUSTOMER_FILE).map { x => x.split('#') }
  val salesRawData = sc.textFile(SALES_FILE).map { x => x.split('#') }
  val customer: RDD[(Long, String)] = customerRawData.map { array => (array(0).toLong, array(3)) }
  val sales: RDD[(Int, Int, Int, Long, Long)] = salesRawData.map {
    array => {
      val dateTime = new DateTime(array(0).toLong * 1000L)
      (dateTime.getYear, dateTime.getMonthOfYear, dateTime.getDayOfMonth, array(1).toLong, array(2).toLong)
    }
  }

  /**
    * This function calculates the total sales made in a year, month and on daily basis
    */
  def getTotalSales: RDD[(Long, (String, String))] = {
    val yearlySalesData = sales.groupBy { tuple => (tuple._1, tuple._4) }
    val monthlySalesData = sales.groupBy { tuple => (tuple._1, tuple._2, tuple._4) }
    val dailySalesData = sales.groupBy { tuple => (tuple._1, tuple._2, tuple._3, tuple._4) }

    val yearlySum = yearlySalesData.map { oneTuple =>
      (oneTuple._1._2, oneTuple._1._1, "#", "#",
        oneTuple._2.foldLeft(0.toLong)((accumulate, tuple) => accumulate + tuple._5))
    }

    val monthlySum = monthlySalesData.map { oneTuple =>
      (oneTuple._1._3, oneTuple._1._1, oneTuple._1._2, "#",
        oneTuple._2.foldLeft(0.toLong)((accumulate, tuple) => accumulate + tuple._5))
    }

    val dailySum = dailySalesData.map { oneTuple =>
      (oneTuple._1._4, oneTuple._1._1, oneTuple._1._2, oneTuple._1._3,
        oneTuple._2.foldLeft(0.toLong)((accumulate, tuple) => accumulate + tuple._5))
    }

    val yearlyResult = yearlySum.map { tuple => (tuple._1, s"#${tuple._2}#${tuple._3}#${tuple._4}#${tuple._5}") }
    val monthlyResult = monthlySum.map { tuple => (tuple._1, s"#${tuple._2}#${tuple._3}#${tuple._4}#${tuple._5}") }
    val dailyResult = dailySum.map { tuple => (tuple._1, s"#${tuple._2}#${tuple._3}#${tuple._4}#${tuple._5}") }

    val yearlyData = customer.join(yearlyResult)
    val monthlyData = customer.join(monthlyResult)
    val dailyData = customer.join(dailyResult)
    yearlyData ++ monthlyData ++ dailyData
  }

  /**
    * This function writes the final result to a file.
    */
  def writeResult(joinedData: RDD[(Long, (String, String))]): Unit = {
    val result = joinedData.map {
      data => data._2._1 + data._2._2
    }.sortBy(x => x)
    result.repartition(1).saveAsTextFile("result")
  }
}
