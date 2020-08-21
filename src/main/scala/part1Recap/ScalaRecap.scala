package part1Recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * @author mkarki
  */
object ScalaRecap extends App {

  // values and variables
  val aBoolean: Boolean = false;

  // expressions
  val anIfExpression = if (2 > 3) "bigger" else "smaller"

  // instructions vs expressions
  val theUnit = println("Hello, scala")

  //oop
  class Animal
  class Dog extends Animal
  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit =
      println("Crunch!!")
  }

  //singleton pattern
  object MySingleton
  //companions
  object Carnivore

  // Functional programming
  val incrementer: Int => Int = x => x + 1
  val incremented = incrementer(42)

  // map, flatMap, filter
  val processedList = List(1, 2, 3).map(incrementer)

  // pattern-matching
  val unknown = 45
  val ordinal = unknown match {
    case 1 => "one"
    case _ => "everything else"
  }

  // Future
  import scala.concurrent.ExecutionContext.Implicits.global
  val aFuture = Future {
    // some expensive computations, runs on another thread
    42
  }

  aFuture.onComplete {
    case Success(value) => println(s"I've found $value")
    case Failure(ex)    => println(s" I've failed : $ex")
  }

  // Partial Functions
  val aPartialFunction: PartialFunction[Int, Int] = {
    case 1 => 43
    case 8 => 56
    case _ => 999
  }
}
