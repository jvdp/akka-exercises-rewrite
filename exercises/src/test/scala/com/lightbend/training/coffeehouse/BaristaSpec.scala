/** Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
  */

package com.lightbend.training.coffeehouse

import akka.testkit.TestProbe
import scala.concurrent.duration.DurationInt
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Sink

class BaristaSpec extends BaseAkkaSpec {

  // *** actor ***
  "Sending PrepareCoffee to Barista" should {
    "result in sending a CoffeePrepared response after prepareCoffeeDuration" in {
      val sender = TestProbe()
      implicit val ref = sender.ref
      // val barista = system.actorOf(Barista.props(100 milliseconds, 100))
      val barista = Barista.flowActor(Barista.flow(100 milliseconds, 100))
      sender.within(50 milliseconds, 200 milliseconds) { // The time is not extremely accurate, so we relax the timing constraints.
        barista ! Barista.PrepareCoffee(Coffee.Akkaccino, system.deadLetters)
        sender.expectMsg(Barista.CoffeePrepared(Coffee.Akkaccino, system.deadLetters))
      }
    }
    "result in sending a CoffeePrepared response with a random Coffee for an inaccurate one" in {
      val waiter = TestProbe()
      val accuracy = 50
      val runs = 1000
      // val barista = system.actorOf(Barista.props(0 milliseconds, accuracy))
      val barista = Barista.flowActor(Barista.flow(0 milliseconds, accuracy))
      val guest = system.deadLetters
      var coffees = List.empty[Coffee]
      for (_ <- 1 to runs) {
        implicit val ref = waiter.ref
        barista ! Barista.PrepareCoffee(Coffee.Akkaccino, guest)
        coffees +:= waiter.expectMsgPF() { case Barista.CoffeePrepared(coffee, `guest`) =>
          coffee
        }
      }
      val expectedCount = runs * accuracy / 100
      val variation = expectedCount / 5
      coffees count (_ == Coffee.Akkaccino) shouldEqual expectedCount +- variation
    }
  }

  // *** flow ***
  "Barista.flow" should {
    "result in sending a CoffeePrepared response after prepareCoffeeDuration" in {
      val ref = TestProbe().ref

      Source
        .single(() -> Barista.PrepareCoffee(Coffee.Akkaccino, ref))
        .via(Barista.flow(100 milliseconds, 100))
        .runWith(Sink.seq)
        .futureValue shouldEqual
        Seq(
          () -> Barista.CoffeePrepared(Coffee.Akkaccino, ref)
        )
    }

    "result in sending a CoffeePrepared response with a random Coffee for an inaccurate one" in {
      val ref = TestProbe().ref
      val accuracy = 50
      val runs = 1000
      val expectedCount = runs * accuracy / 100
      val variation = expectedCount / 5

      Source
        .repeat(() -> Barista.PrepareCoffee(Coffee.Akkaccino, ref))
        .take(runs)
        .via(Barista.flow(0 milliseconds, accuracy))
        .runWith(Sink.seq)
        .futureValue
        .count(_._2.coffee == Coffee.Akkaccino) shouldEqual expectedCount +- variation
    }
  }
}
