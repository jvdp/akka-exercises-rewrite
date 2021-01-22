/**
 * Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
 */

package com.lightbend.training.coffeehouse

import akka.actor.{ Actor, ActorLogging }

class CoffeeHouse extends Actor with ActorLogging {

  override def receive: Receive = {
    case _ => log.info("Coffee Brewing")
  }
}
