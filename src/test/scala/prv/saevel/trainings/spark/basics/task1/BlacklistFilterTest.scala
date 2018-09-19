package prv.saevel.trainings.spark.basics.task1

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.junit.JUnitRunner
import prv.saevel.trainings.spark.basics.Customer

@RunWith(classOf[JUnitRunner])
class BlacklistFilterTest extends WordSpec with Matchers with SparkTestSupport with StaticPropertyChecks {

  private def customers(ageGenerator: Gen[Int], balanceGenerator: Gen[Double]): Gen[List[Customer]] =
    Gen.choose(0, 100).flatMap(n => Gen.listOfN(n , for {
    id <- Gen.choose(0, Long.MaxValue)
    name <- Gen.oneOf("John", "Edward", "Patrick", "William", "Stephen", "Anne", "Sophie", "Julia", "Alice", "Mary")
    surname <- Gen.oneOf("Smith", "Johnson", "Williams", "Evans", "Douglas")
    age <- ageGenerator
    balance <- balanceGenerator
  } yield Customer(id, name, surname, age, balance)))

  private val correctCustomers = customers(Gen.choose(18, 30), Gen.choose(15000, Double.MaxValue))

  private val incorrectCustomers = customers(Gen.choose(31, 100), Gen.choose(-15000, 15000))

  "BlacklistFiler" when {

    "given a list of Customers as CSV and a blacklist of users as CSV" should {

      "deserialize Customers, filter out those above 30 and below 15000$ as well as remove blacklisted ones" in {

        withSparkContext("BlacklistFilterTest"){ implicit sparkContext =>

          forOneOf(correctCustomers, incorrectCustomers){ (correct, incorrect) =>

            val sparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("BlacklistFilterTest"))

            val allCustomersRDD = sparkContext.parallelize(correct ++ incorrect)

            val blacklisted = allCustomersRDD.sample(false, 0.3)

            val allCustomersCsv = allCustomersRDD.map(toCsv)
            val blacklistedCsv = blacklisted.map(toCsv)

            val results = BlacklistFilter(allCustomersCsv, blacklistedCsv).collect

            val expectedResults = sparkContext.parallelize(correct).subtract(blacklisted).collect

            results should contain theSameElementsAs(expectedResults)
          }
        }
      }
    }
  }

  private def toCsv(customer: Customer): String =
    List(customer.id, customer.name, customer.surname, customer.age, customer.accountBalance).mkString(", ")
}
