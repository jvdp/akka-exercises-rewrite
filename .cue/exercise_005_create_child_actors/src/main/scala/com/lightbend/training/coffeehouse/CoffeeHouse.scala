/**
 * Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
 */

package com.lightbend.training.coffeehouse

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }

object CoffeeHouse {

  case object CreateGuest

  def props: Props =
    Props(new CoffeeHouse)
}

class CoffeeHouse extends Actor with ActorLogging {

  import CoffeeHouse._

  log.debug("CoffeeHouse Open")

  override def receive: Receive = {
    case CreateGuest => createGuest()
  }

  protected def createGuest(): ActorRef =
    context.actorOf(Guest.props)
}
