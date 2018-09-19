package prv.saevel.trainings.spark.basics.task1

import org.scalacheck.Gen
import org.scalatest.TestSuite

trait StaticPropertyChecks { self: TestSuite =>

  protected def maxRetries = 10

  protected def forOneOf[X](g: Gen[X])(f: X => Unit): Unit = {
    g.retryUntil(_ => true, maxRetries).sample match {
      case Some(data) => f(data)
      case None => fail(s"Impossible to generate input data in $maxRetries retries.")
    }
  }

  protected def forOneOf[X, Y](g1: Gen[X], g2: Gen[Y])(f: (X, Y) => Unit): Unit = forOneOf(for{
    d1 <- g1
    d2 <- g2
  } yield (d1, d2))(f.tupled)

  protected def forOneOf[X, Y, Z](g1: Gen[X], g2: Gen[Y], g3: Gen[Z])(f: (X, Y, Z) => Unit): Unit = forOneOf(for{
    d1 <- g1
    d2 <- g2
    d3 <- g3
  } yield (d1, d2, d3))(f.tupled)

  protected def forOneOf[X, Y, Z, V](g1: Gen[X], g2: Gen[Y], g3: Gen[Z], g4: Gen[V])(f: (X, Y, Z, V) => Unit): Unit = forOneOf(for{
    d1 <- g1
    d2 <- g2
    d3 <- g3
    d4 <- g4
  } yield (d1, d2, d3, d4))(f.tupled)
}
