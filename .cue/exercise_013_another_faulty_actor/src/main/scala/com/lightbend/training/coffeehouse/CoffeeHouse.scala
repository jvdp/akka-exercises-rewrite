/**
 * Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
 */

package com.lightbend.training.coffeehouse

import akka.actor.{ Actor, ActorLogging, ActorRef, OneForOneStrategy, Props, SupervisorStrategy, Terminated }
import scala.concurrent.duration.{ Duration, MILLISECONDS => Millis }

object CoffeeHouse {

  case class CreateGuest(favoriteCoffee: Coffee, caffeineLimit: Int)
  case class ApproveCoffee(coffee: Coffee, guest: ActorRef)

  def props(caffeineLimit: Int): Props =
    Props(new CoffeeHouse(caffeineLimit))
}

class CoffeeHouse(caffeineLimit: Int) extends Actor with ActorLogging {

  import CoffeeHouse._

  override val supervisorStrategy: SupervisorStrategy = {
    val decider: SupervisorStrategy.Decider = {
      case Guest.CaffeineException => SupervisorStrategy.Stop
    }
    OneForOneStrategy()(decider orElse super.supervisorStrategy.decider)
  }
  private val baristaAccuracy = context.system.settings.config getInt "coffee-house.barista.accuracy"
  private val baristaPrepareCoffeeDuration =
    Duration(context.system.settings.config.getDuration("coffee-house.barista.prepare-coffee-duration", Millis), Millis)
  private val guestFinishCoffeeDuration =
    Duration(context.system.settings.config.getDuration("coffee-house.guest.finish-coffee-duration", Millis), Millis)
  private val waiterMaxComplaintCount = context.system.settings.config getInt "coffee-house.waiter.max-complaint-count"

  private val barista = createBarista()
  private val waiter = createWaiter()

  private var guestBook = Map.empty[ActorRef, Int] withDefaultValue 0

  log.debug("CoffeeHouse Open")

  override def receive: Receive = {
    case CreateGuest(favoriteCoffee, caffeineLimit) =>
      val guest: ActorRef = createGuest(favoriteCoffee, caffeineLimit)
      guestBook += guest -> 0
      log.info(s"Guest $guest added to guest book.")
      context.watch(guest)
    case ApproveCoffee(coffee, guest) if guestBook(guest) < caffeineLimit =>
      guestBook += guest -> (guestBook(guest) + 1)
      log.info(s"Guest $guest caffeine count incremented.")
      barista forward Barista.PrepareCoffee(coffee, guest)
    case ApproveCoffee(coffee, guest) =>
      log.info(s"Sorry, $guest, but you have reached your limit.")
      context.stop(guest)
    case Terminated(guest) =>
      log.info(s"Thanks, $guest, for being our guest!")
      guestBook -= guest

  }

  protected def createBarista(): ActorRef =
    context.actorOf(Barista.props(baristaPrepareCoffeeDuration, baristaAccuracy), "barista")

  protected def createWaiter(): ActorRef =
    context.actorOf(Waiter.props(self, barista, waiterMaxComplaintCount), "waiter")

  protected def createGuest(favoriteCoffee: Coffee, caffeineLimit: Int): ActorRef =
    context.actorOf(Guest.props(waiter, favoriteCoffee, guestFinishCoffeeDuration, caffeineLimit))
}
