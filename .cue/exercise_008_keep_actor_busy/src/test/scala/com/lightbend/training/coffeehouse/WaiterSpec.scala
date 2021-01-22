/**
 * Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
 */

package com.lightbend.training.coffeehouse

import akka.testkit.TestProbe

class WaiterSpec extends BaseAkkaSpec {

  "Sending ServeCoffee to Waiter" should {
    "result in sending PrepareCoffee to Barista" in {
      val barista = TestProbe()
      val guest = TestProbe()
      implicit val ref = guest.ref
      val waiter = system.actorOf(Waiter.props(barista.ref))
      waiter ! Waiter.ServeCoffee(Coffee.Akkaccino)
      barista.expectMsg(Barista.PrepareCoffee(Coffee.Akkaccino, guest.ref))
    }
  }
}
