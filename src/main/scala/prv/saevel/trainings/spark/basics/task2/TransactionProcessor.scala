package prv.saevel.trainings.spark.basics.task2

import org.apache.spark.SparkContext

object TransactionProcessor {

  def run(config: TransactionProcessorConfiguration)(implicit sparkContext: SparkContext): Unit = {
    val customers = sparkContext.textFile(config.customersFile)
      .map(_.split(","))
      .filter(_.size >= 3)
      .map(split => Customer(split(0).toLong, split(1), split(2)))
      .keyBy(_.id)
      .cache

    val accounts = sparkContext.textFile(config.accountsFile)
      .map(_.split(","))
      .filter(_.size >= 3)
      .map(split => Account(split(0).toLong, split(1).toLong, split(2).toDouble))
      .cache

    val transactions = sparkContext.textFile(config.transactionsFile)
      .map(_.split(","))
      .filter(_.size >= 3)
      .map(split => Transaction(split(0).toLong, split(1).toLong, split(2).toDouble))
      // INFO: You could add some custom partitioning here to optimize :)
      .groupBy(_.accountId)

    val suspiciousAccounts = accounts.keyBy(_.id).leftOuterJoin(transactions).filter{ case (_, (account, optionalTransactions)) =>
      optionalTransactions.fold(account.balance != 0.0)(transactions =>
        transactions.map(_.amount).fold(0.0)(_ + _) != account.balance
      )
    }.map {case(_, (account, _)) => account}.cache

    val debitedAccounts = accounts.subtract(suspiciousAccounts).filter(_.balance < 0.0)

    customers.join(debitedAccounts.keyBy(_.customerId))
      .cache
      .map{ case (_, (customer, _)) => customer.id.toString}
      .distinct
      .saveAsTextFile(config.suspiciousCustomersFile)

    customers.join(debitedAccounts.keyBy(_.customerId))
      .cache
      .map{ case (_, (customer, _)) => customer.id.toString}
      .distinct
      .saveAsTextFile(config.customersWithDebitFile)
  }
}
