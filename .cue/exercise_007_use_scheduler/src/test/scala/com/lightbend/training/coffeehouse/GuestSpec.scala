/**
 * Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
 */

package com.lightbend.training.coffeehouse

import akka.testkit.{ EventFilter, TestProbe }
import scala.concurrent.duration.DurationInt

class GuestSpec extends BaseAkkaSpec {

  "Sending CoffeeServed to Guest" should {
    "result in increasing the coffeeCount and log a status message at info" in {
      val guest = system.actorOf(Guest.props(system.deadLetters, Coffee.Akkaccino, 100 milliseconds))
      EventFilter.info(source = guest.path.toString, pattern = """.*[Ee]njoy.*1\.*""", occurrences = 1) intercept {
        guest ! Waiter.CoffeeServed(Coffee.Akkaccino)
      }
    }
    "result in sending ServeCoffee to Waiter after finishCoffeeDuration" in {
      val waiter = TestProbe()
      val guest = createGuest(waiter)
      waiter.within(50 milliseconds, 200 milliseconds) { // The timer is not extremely accurate, relax the timing constraints.
        guest ! Waiter.CoffeeServed(Coffee.Akkaccino)
        waiter.expectMsg(Waiter.ServeCoffee(Coffee.Akkaccino))
      }
    }
  }

  "Sending CoffeeFinished to Guest" should {
    "result in sending ServeCoffee to Waiter" in {
      val waiter = TestProbe()
      val guest = createGuest(waiter)
      guest ! Guest.CoffeeFinished
      waiter.expectMsg(Waiter.ServeCoffee(Coffee.Akkaccino))
    }
  }

  def createGuest(waiter: TestProbe) = {
    val guest = system.actorOf(Guest.props(waiter.ref, Coffee.Akkaccino, 100 milliseconds))
    waiter.expectMsg(Waiter.ServeCoffee(Coffee.Akkaccino)) // Creating Guest immediately sends Waiter.ServeCoffee
    guest
  }
}
