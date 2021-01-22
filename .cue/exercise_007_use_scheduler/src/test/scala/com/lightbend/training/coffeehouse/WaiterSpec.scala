/**
 * Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
 */

package com.lightbend.training.coffeehouse

import akka.testkit.TestProbe

class WaiterSpec extends BaseAkkaSpec {

  "Sending ServeCoffee to Waiter" should {
    "result in sending a CoffeeServed response to sender" in {
      val sender = TestProbe()
      implicit val ref = sender.ref
      val waiter = system.actorOf(Waiter.props)
      waiter ! Waiter.ServeCoffee(Coffee.Akkaccino)
      sender.expectMsg(Waiter.CoffeeServed(Coffee.Akkaccino))
    }
  }
}
