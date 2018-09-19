package prv.saevel.trainings.spark.basics.task1

import org.apache.spark.rdd.RDD
import prv.saevel.trainings.spark.basics.Customer

object BlacklistFilter {

  def apply(allCustomers: RDD[String], blacklist: RDD[String]): RDD[Customer] = ???
}
