/**
 * Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
 */

package com.lightbend.training.coffeehouse

import akka.actor.{ Actor, ActorRef, Props }

object Waiter {

  case class ServeCoffee(coffee: Coffee)
  case class CoffeeServed(coffee: Coffee)

  def props(coffeeHouse: ActorRef): Props =
    Props(new Waiter(coffeeHouse))
}

class Waiter(coffeeHouse: ActorRef) extends Actor {

  import Waiter._

  override def receive: Receive = {
    case ServeCoffee(coffee)                   => coffeeHouse ! CoffeeHouse.ApproveCoffee(coffee, sender())
    case Barista.CoffeePrepared(coffee, guest) => guest ! CoffeeServed(coffee)
  }
}
