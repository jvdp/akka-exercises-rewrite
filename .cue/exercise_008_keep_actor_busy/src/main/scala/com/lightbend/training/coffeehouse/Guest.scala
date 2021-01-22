/**
 * Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
 */

package com.lightbend.training.coffeehouse

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Timers}

import scala.concurrent.duration.FiniteDuration

object Guest {

  case object CoffeeFinished

  def props(waiter: ActorRef, favoriteCoffee: Coffee, finishCoffeeDuration: FiniteDuration): Props =
    Props(new Guest(waiter, favoriteCoffee, finishCoffeeDuration))
}

class Guest(waiter: ActorRef, favoriteCoffee: Coffee, finishCoffeeDuration: FiniteDuration)
  extends Actor with ActorLogging with Timers {

  import Guest._

  private var coffeeCount = 0

  orderFavoriteCoffee()

  override def receive: Receive = {
    case Waiter.CoffeeServed(coffee) =>
      coffeeCount += 1
      log.info("Enjoying my {} yummy {}!", coffeeCount, coffee)
      timers.startSingleTimer("coffee-finished", CoffeeFinished, finishCoffeeDuration)
    case CoffeeFinished =>
      orderFavoriteCoffee()
  }

  private def orderFavoriteCoffee(): Unit =
    waiter ! Waiter.ServeCoffee(favoriteCoffee)
}
