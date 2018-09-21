package prv.saevel.trainings.spark.basics

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import prv.saevel.trainings.spark.basics.task1.Customer
import prv.saevel.trainings.spark.basics.task2.Account

object Random {

  def produce(implicit sparkContext: SparkContext): Unit = {

    /*
    val richYoungCustomers = sparkContext.textFile("customers.txt")
      .map(_.split(", "))
      .map(splits => Customer(splits(0).toLong, splits(1), splits(2), splits(3).toInt, splits(4).toDouble))
      .filter(_.age > 30)
      .filter(_.accountBalance > 15000)

    val poorYoungCustomers = sparkContext.textFile("customers.txt")
      .map(_.split(", "))
      .map(splits => Customer(splits(0).toLong, splits(1), splits(2), splits(3).toInt, splits(4).toDouble))
      .filter(_.age > 30)
      .filter(_.accountBalance < 500)



    val youngCustomers = sparkContext.textFile("customers.txt")
      .map(_.split(", "))
      .map(splits => Customer(splits(0).toLong, splits(1), splits(2), splits(3).toInt, splits(4).toDouble))
      .filter(_.age > 30)
      .cache()

    val customers: RDD[(Long, Customer)]
    val accounts: RDD[(Long, Account)]

    customers.save

    val joinResult: RDD[(Long, (Customer, Account))] = customers.join(accounts)

    val leftJoinResult: RDD[(Long, (Customer, Option[Account]))] = customers.leftOuterJoin(accounts)

    val rightJoinResult: RDD[(Long, (Option[Customer], Account))] = customers.rightOuterJoin(accounts)

    val fullJoinResult: RDD[(Long, (Option[Customer], Option[Account]))] = customers.fullOuterJoin(accounts)

    // accounts.keyBy(_.customerId).join(youngCustomers.keyBy(_.id))

    val richYoungCustomers = youngCustomers.filter(_.accountBalance > 15000)
    val poorYoungCustomers = youngCustomers.filter(_.accountBalance < 500)

    */
  }
}
