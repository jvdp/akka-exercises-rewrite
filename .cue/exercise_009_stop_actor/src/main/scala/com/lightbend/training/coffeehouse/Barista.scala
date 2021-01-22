/**
 * Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
 */

package com.lightbend.training.coffeehouse

import akka.actor.{ Actor, ActorRef, Props }
import scala.concurrent.duration.FiniteDuration

object Barista {

  case class PrepareCoffee(coffee: Coffee, guest: ActorRef)
  case class CoffeePrepared(coffee: Coffee, guest: ActorRef)

  def props(prepareCoffeeDuration: FiniteDuration): Props =
    Props(new Barista(prepareCoffeeDuration))
}

class Barista(prepareCoffeeDuration: FiniteDuration) extends Actor {

  import Barista._

  override def receive: Receive = {
    case PrepareCoffee(coffee, guest) =>
      busy(prepareCoffeeDuration)
      sender() ! CoffeePrepared(coffee, guest)
  }
}
