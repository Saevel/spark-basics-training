package prv.saevel.trainings.spark.basics.task1

import org.apache.spark.rdd.RDD

object BlacklistFilter {

  def apply(allCustomers: RDD[String], blacklist: RDD[String]): RDD[Customer] = ???
}
