/**
 * Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
 */

package com.lightbend.training.coffeehouse

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import scala.concurrent.duration.{ Duration, MILLISECONDS => Millis }

object CoffeeHouse {

  case class CreateGuest(favoriteCoffee: Coffee)

  def props: Props =
    Props(new CoffeeHouse)
}

class CoffeeHouse extends Actor with ActorLogging {

  import CoffeeHouse._

  private val baristaPrepareCoffeeDuration =
    Duration(context.system.settings.config.getDuration("coffee-house.barista.prepare-coffee-duration", Millis), Millis)
  private val guestFinishCoffeeDuration =
    Duration(context.system.settings.config.getDuration("coffee-house.guest.finish-coffee-duration", Millis), Millis)

  private val barista = createBarista()
  private val waiter = createWaiter()

  log.debug("CoffeeHouse Open")

  override def receive: Receive = {
    case CreateGuest(favoriteCoffee) => createGuest(favoriteCoffee)
  }

  protected def createBarista(): ActorRef =
    context.actorOf(Barista.props(baristaPrepareCoffeeDuration), "barista")

  protected def createWaiter(): ActorRef =
    context.actorOf(Waiter.props(barista), "waiter")

  protected def createGuest(favoriteCoffee: Coffee): ActorRef =
    context.actorOf(Guest.props(waiter, favoriteCoffee, guestFinishCoffeeDuration))
}
