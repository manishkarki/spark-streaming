package part1Recap

/**
  * @author mkarki
  */
object ScalaRecap {

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
}
