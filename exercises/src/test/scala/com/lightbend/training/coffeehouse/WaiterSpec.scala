/** Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
  */

package com.lightbend.training.coffeehouse

import akka.testkit.{EventFilter, TestProbe}
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Sink
import akka.actor.ActorRef

class WaiterSpec extends BaseAkkaSpec {

  // *** actor ***
  "Sending ServeCoffee to Waiter" should {
    "result in sending ApproveCoffee to CoffeeHouse" in {
      val coffeeHouse = TestProbe()
      val guest = TestProbe()
      implicit val ref = guest.ref
      val waiter = Waiter.flowActor(coffeeHouse.ref, system.deadLetters, Waiter.flow(Int.MaxValue))
      waiter ! Waiter.ServeCoffee(Coffee.Akkaccino)
      coffeeHouse.expectMsg(CoffeeHouse.ApproveCoffee(Coffee.Akkaccino, guest.ref))
    }
  }

  "Sending Complaint to Waiter" should {
    "result in sending PrepareCoffee to Barista" in {
      val barista = TestProbe()
      val guest = TestProbe()
      implicit val ref = guest.ref
      val waiter = Waiter.flowActor(system.deadLetters, barista.ref, Waiter.flow(1))
      waiter ! Waiter.Complaint(Coffee.Akkaccino)
      barista.expectMsg(Barista.PrepareCoffee(Coffee.Akkaccino, guest.ref))
    }

    "result in a FrustratedException if maxComplaintCount exceeded" in {
      val waiter = Waiter.flowActor(system.deadLetters, system.deadLetters, Waiter.flow(0))
      EventFilter[Waiter.FrustratedException[ActorRef]](occurrences = 1) intercept {
        waiter ! Waiter.Complaint(Coffee.Akkaccino)
      }
    }
  }

  // *** flow ***
  "Waiter.flow" should {
    "result in sending ApproveCoffee to CoffeeHouse" in {
      val ref = TestProbe().ref

      Source
        .single(ref -> Left(Waiter.ServeCoffee(Coffee.Akkaccino)))
        .via(Waiter.flow(Int.MaxValue))
        .runWith(Sink.seq)
        .futureValue shouldEqual
        Seq(
          Left(CoffeeHouse.ApproveCoffee(Coffee.Akkaccino, ref))
        )
    }

    "result in sending PrepareCoffee to Barista" in {
      val ref = TestProbe().ref

      Source
        .single(ref -> Right(Right(Waiter.Complaint(Coffee.Akkaccino))))
        .via(Waiter.flow(1))
        .runWith(Sink.seq)
        .futureValue shouldEqual
        Seq(
          Right(Right(Barista.PrepareCoffee(Coffee.Akkaccino, ref)))
        )
    }

    "result in a FrustratedException if maxComplaintCount exceeded" in {
      val ref = TestProbe().ref

      Source
        .single(ref -> Right(Right(Waiter.Complaint(Coffee.Akkaccino))))
        .via(Waiter.flow(0))
        .runWith(Sink.seq)
        .failed
        .futureValue should be(a[Waiter.FrustratedException[_]])
    }
  }
}
