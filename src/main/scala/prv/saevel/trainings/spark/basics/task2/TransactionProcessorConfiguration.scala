package prv.saevel.trainings.spark.basics.task2

case class TransactionProcessorConfiguration(accountsFile: String,
                                             customersFile: String,
                                             transactionsFile: String,
                                             suspiciousCustomersFile: String,
                                             customersWithDebitFile: String)
