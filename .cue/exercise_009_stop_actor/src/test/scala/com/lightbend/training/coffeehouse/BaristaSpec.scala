/**
 * Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
 */

package com.lightbend.training.coffeehouse

import akka.testkit.TestProbe
import scala.concurrent.duration.DurationInt

class BaristaSpec extends BaseAkkaSpec {

  "Sending PrepareCoffee to Barista" should {
    "result in sending a CoffeePrepared response after prepareCoffeeDuration" in {
      val sender = TestProbe()
      implicit val ref = sender.ref
      val barista = system.actorOf(Barista.props(100 milliseconds))
      sender.within(50 milliseconds, 1000 milliseconds) { // busy is innccurate, so we relax the timing constraints.
        barista ! Barista.PrepareCoffee(Coffee.Akkaccino, system.deadLetters)
        sender.expectMsg(Barista.CoffeePrepared(Coffee.Akkaccino, system.deadLetters))
      }
    }
  }
}
