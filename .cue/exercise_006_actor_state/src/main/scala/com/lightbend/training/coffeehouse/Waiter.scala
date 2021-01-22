/**
 * Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
 */

package com.lightbend.training.coffeehouse

import akka.actor.{ Actor, Props }

object Waiter {

  case class ServeCoffee(coffee: Coffee)
  case class CoffeeServed(coffee: Coffee)

  def props: Props =
    Props(new Waiter)
}

class Waiter extends Actor {

  import Waiter._

  override def receive: Receive = {
    case ServeCoffee(coffee) => sender() ! CoffeeServed(coffee)
  }
}
