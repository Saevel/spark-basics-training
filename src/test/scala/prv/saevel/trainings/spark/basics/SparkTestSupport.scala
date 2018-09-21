package prv.saevel.trainings.spark.basics

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

trait SparkTestSupport {

  def withSparkContext[T](appName: String)(f: SparkContext => T): T = {
    val tryContext = Try(new SparkContext(new SparkConf().setMaster("local[*]").setAppName(appName)))
    val tryResult = tryContext.map(f)

    tryContext.foreach(_.stop)
    tryResult match {
      case Success(t) => t
      case Failure(e) => throw e
    }
  }
}
