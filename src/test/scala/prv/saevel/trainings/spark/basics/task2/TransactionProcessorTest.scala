package prv.saevel.trainings.spark.basics.task2

import java.nio.file.{Files, Paths}

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}
import org.scalatest.junit.JUnitRunner
import prv.saevel.trainings.spark.basics.{FileUtils, StaticPropertyChecks}

@RunWith(classOf[JUnitRunner])
class TransactionProcessorTest extends WordSpec with Matchers with StaticPropertyChecks with BeforeAndAfter with FileUtils {

  private val config = TransactionProcessorConfiguration(
   "target/accounts",
    "target/customers",
    "target/transactions",
    "target/suspicious",
    "target/debits"
  )

  private val ids2: Gen[Long] = Gen.choose(0, 999)

  private val ids: Gen[Long] = Gen.choose(100000, 999999)

  private val customerCount = 5

  private val maxAccountsPerCustomer = 2

  private val maxTransactionPerAccount = 4

  before {

    Files.createDirectories(Paths.get(System.getProperty("user.dir")).resolve("target"))

    deleteDirectoryIfExists(config.accountsFile)
    deleteDirectoryIfExists(config.customersFile)
    deleteDirectoryIfExists(config.transactionsFile)
    deleteDirectoryIfExists(config.customersWithDebitFile)
    deleteDirectoryIfExists(config.suspiciousCustomersFile)
  }

  private def transactions(size: Int, accountIdGenerator: Gen[Long]): Gen[List[Transaction]] = Gen.listOfN(size , for {
    id <- ids
    acccountId <- accountIdGenerator
    amount <- Gen.choose(-1000.0, 1000.0)
  } yield Transaction(id, acccountId, amount))

  private def nonSuspiciousAccounts(customerIdGeneraotr: Gen[Long],
                              accountsPerCustomer: Int,
                              transactionsPerAccounts: Int,
                              transactionsGenerator: (Int, Gen[Long]) => Gen[List[Transaction]]): Gen[List[(Account, List[Transaction])]] =
    Gen.listOfN(accountsPerCustomer, for {
      accountId <- ids2
      transactions <- transactionsGenerator(transactionsPerAccounts, Gen.const(accountId))
      customerId <- customerIdGeneraotr
    } yield (Account(accountId, customerId, balanceFromTransactions(transactions)), transactions))

  private def suspiciousAccounts(customerIdGeneraotr: Gen[Long],
                                accountsPerCustomer: Int,
                                transactionsPerAccounts: Int,
                                transactionsGenerator: (Int, Gen[Long]) => Gen[List[Transaction]]): Gen[List[(Account, List[Transaction])]] =
    Gen.listOfN(accountsPerCustomer, for {
      accountId <- ids
      transactions <- transactionsGenerator(transactionsPerAccounts, Gen.const(accountId))
      customerId <- customerIdGeneraotr
      balance <- Gen.choose(-15000.0, 15000.0)
    } yield (Account(accountId, customerId, balance), transactions))

  private val suspiciousCustomers: Gen[List[(Customer, List[(Account, List[Transaction])])]] = Gen.listOfN(customerCount, for {
    id <- ids
    accounts <- suspiciousAccounts(Gen.const(id), maxAccountsPerCustomer, maxTransactionPerAccount, transactions(_, _))
    name <- Gen.alphaStr
    surname <- Gen.alphaStr
  } yield (Customer(id, name, surname), accounts))

  private val nonSuspiciousCustomers: Gen[List[(Customer, List[(Account, List[Transaction])])]] = Gen.listOfN(customerCount, for {
    id <- ids2
    accounts <- nonSuspiciousAccounts(Gen.const(id), maxAccountsPerCustomer, maxTransactionPerAccount, transactions(_, _))
    name <- Gen.alphaStr
    surname <- Gen.alphaStr
  } yield (Customer(id, name, surname), accounts))


  "TransactionProcessor" when {

    "given a mixture of 'suspicious', 'with debit' and other Customers" should {

      "save 'suspicious' ones ids to one file and 'with debit' ones to another file" in forOneOf(nonSuspiciousCustomers, suspiciousCustomers) { (nonSuspicious, suspicious) =>

        var suspiciousCustomers: List[Customer] = List.empty
        var customersWithDebit: List[Customer] = List.empty

        var allCustomers: List[Customer] = List.empty
        var allAccounts: List[Account] = List.empty
        var allTransactions: List[Transaction] = List.empty

        suspicious.foreach{case (customer, accountsWithTransactions) =>
            allCustomers = allCustomers :+ customer
            suspiciousCustomers = suspiciousCustomers :+ customer

            accountsWithTransactions.foreach { case (account, transactions) =>
              allAccounts = allAccounts :+ account
              allTransactions = allTransactions ++ transactions
            }
        }

        nonSuspicious.foreach{case (customer, accountsWithTransactions) =>
          allCustomers = allCustomers :+ customer

          val credit = accountsWithTransactions.map(_._1.balance).fold(0.0)(_ + _)

          if(credit < 0.0){
            customersWithDebit = customersWithDebit :+ customer
          }

          accountsWithTransactions.foreach { case (account, transactions) =>
            allAccounts = allAccounts :+ account
            allTransactions = allTransactions ++ transactions
          }
        }

        implicit val sparkContext = new SparkContext(new SparkConf().setAppName("TransactionProcessorTest").setMaster("local[*]"))

        sparkContext.parallelize(allCustomers).map(c => s"${c.id},${c.name},${c.surname}").coalesce(1).saveAsTextFile(config.customersFile)
        sparkContext.parallelize(allAccounts).map(a => s"${a.id},${a.customerId},${a.balance}").coalesce(1).saveAsTextFile(config.accountsFile)
        sparkContext.parallelize(allTransactions).map(t => s"${t.id},${t.accountId},${t.amount}").coalesce(1).saveAsTextFile(config.transactionsFile)

        TransactionProcessor.run(config)

        val detectedSuspicious = sparkContext.textFile(config.suspiciousCustomersFile).map(_.toLong).collect
        val detectedDebits = sparkContext.textFile(config.customersWithDebitFile).map(_.toLong).collect

        detectedSuspicious should contain theSameElementsAs(suspiciousCustomers.map(_.id))
        detectedDebits should contain theSameElementsAs(customersWithDebit.map(_.id))
      }
    }
  }

  private def balanceFromTransactions(transactions: Traversable[Transaction]): Double =
    transactions.map(_.amount).fold(0.0)(_ + _)
}
