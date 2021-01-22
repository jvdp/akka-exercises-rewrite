/**
 * Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
 */

package com.lightbend.training.coffeehouse

import akka.actor.{ Actor, Props }

object Guest {

  def props: Props =
    Props(new Guest)
}

class Guest extends Actor {

  override def receive: Receive =
    Actor.emptyBehavior
}
