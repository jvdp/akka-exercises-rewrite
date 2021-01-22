/**
 * Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
 */

package com.lightbend.training.coffeehouse

import akka.actor.{ Actor, ActorRef, Props }

object Waiter {

  case class ServeCoffee(coffee: Coffee)
  case class CoffeeServed(coffee: Coffee)

  def props(barista: ActorRef): Props =
    Props(new Waiter(barista))
}

class Waiter(barista: ActorRef) extends Actor {

  import Waiter._

  override def receive: Receive = {
    case ServeCoffee(coffee)                   => barista ! Barista.PrepareCoffee(coffee, sender())
    case Barista.CoffeePrepared(coffee, guest) => guest ! CoffeeServed(coffee)
  }
}
