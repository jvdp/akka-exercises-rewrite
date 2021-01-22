/**
 * Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
 */

package com.lightbend.training.coffeehouse

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }

object CoffeeHouse {

  case class CreateGuest(favoriteCoffee: Coffee)

  def props: Props =
    Props(new CoffeeHouse)
}

class CoffeeHouse extends Actor with ActorLogging {

  import CoffeeHouse._

  private val waiter = createWaiter()

  log.debug("CoffeeHouse Open")

  override def receive: Receive = {
    case CreateGuest(favoriteCoffee) => createGuest(favoriteCoffee)
  }

  protected def createWaiter(): ActorRef =
    context.actorOf(Waiter.props, "waiter")

  protected def createGuest(favoriteCoffee: Coffee): ActorRef =
    context.actorOf(Guest.props(waiter, favoriteCoffee))
}
