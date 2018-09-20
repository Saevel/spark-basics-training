package prv.saevel.trainings.spark.basics.task1

import org.apache.spark.rdd.RDD
import prv.saevel.trainings.spark.basics.Customer

object BlacklistFilter {

  def apply(allCustomers: RDD[String], blacklist: RDD[String]): RDD[Customer] = {
    allCustomers
      .map(fromCsv)
      .subtract(blacklist.map(fromCsv))
      .filter(_.age < 30)
      .filter(_.accountBalance > 15000)
  }

  private def fromCsv(line: String): Customer = {
    val splitLine = line.split(", ")
    Customer(splitLine(0).toLong, splitLine(1), splitLine(2), splitLine(3).toInt, splitLine(4).toDouble)
  }
}
