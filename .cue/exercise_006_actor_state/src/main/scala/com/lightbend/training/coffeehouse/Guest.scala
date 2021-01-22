/**
 * Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
 */

package com.lightbend.training.coffeehouse

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }

object Guest {

  case object CoffeeFinished

  def props(waiter: ActorRef, favoriteCoffee: Coffee): Props =
    Props(new Guest(waiter, favoriteCoffee))
}

class Guest(waiter: ActorRef, favoriteCoffee: Coffee) extends Actor with ActorLogging {

  import Guest._

  private var coffeeCount = 0

  override def receive: Receive = {
    case Waiter.CoffeeServed(coffee) =>
      coffeeCount += 1
      log.info("Enjoying my {} yummy {}!", coffeeCount, coffee)
    case CoffeeFinished =>
      waiter ! Waiter.ServeCoffee(favoriteCoffee)
  }
}
