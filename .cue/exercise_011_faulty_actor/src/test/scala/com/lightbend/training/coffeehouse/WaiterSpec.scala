/**
 * Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
 */

package com.lightbend.training.coffeehouse

import akka.testkit.TestProbe

class WaiterSpec extends BaseAkkaSpec {

  "Sending ServeCoffee to Waiter" should {
    "result in sending ApproveCoffee to CoffeeHouse" in {
      val coffeeHouse = TestProbe()
      val guest = TestProbe()
      implicit val ref = guest.ref
      val waiter = system.actorOf(Waiter.props(coffeeHouse.ref))
      waiter ! Waiter.ServeCoffee(Coffee.Akkaccino)
      coffeeHouse.expectMsg(CoffeeHouse.ApproveCoffee(Coffee.Akkaccino, guest.ref))
    }
  }
}
