/**
 * Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
 */

package com.lightbend.training.coffeehouse

import akka.actor.{ Actor, ActorLogging, Props }

object CoffeeHouse {

  def props: Props =
    Props(new CoffeeHouse)
}

class CoffeeHouse extends Actor with ActorLogging {

  log.debug("CoffeeHouse Open")

  override def receive: Receive = {
    case _ => log.info("Coffee Brewing")
  }
}
