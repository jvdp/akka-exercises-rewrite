/** Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
  */

package com.lightbend.training.coffeehouse

import akka.actor.{Actor, ActorRef, Props}
import akka.stream.scaladsl.Flow
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Keep
import scala.util.Success
import akka.actor.PoisonPill
import scala.util.Failure
import akka.Done

object Waiter {

  case class ServeCoffee(coffee: Coffee)
  case class CoffeeServed(coffee: Coffee)
  case class Complaint(coffee: Coffee)
  case class FrustratedException[K](coffee: Coffee, guest: K) extends IllegalStateException("Too many complaints!")

  def props(coffeeHouse: ActorRef, barista: ActorRef, maxComplaintCount: Int): Props =
    Props(new Waiter(coffeeHouse, barista, maxComplaintCount))

  type Request[K] = Either[Barista.CoffeePrepared[K], (K, Either[Complaint, ServeCoffee])]
  type Response[K] = Either[(K, CoffeeServed), Either[CoffeeHouse.ApproveCoffee[K], Barista.PrepareCoffee[K]]]

  def flow[K](maxComplaintCount: Int): Flow[Request[K], Response[K], NotUsed] =
    Flow[Request[K]]
      .scan((0, Option.empty[Response[K]])) {
        case (complaintCount -> _, Left((Barista.CoffeePrepared(coffee, guest)))) =>
          complaintCount -> Some(Left(guest -> CoffeeServed(coffee)))

        case (complaintCount -> _, Right(guest -> Right((ServeCoffee(coffee))))) =>
          complaintCount -> Some(Right(Left(CoffeeHouse.ApproveCoffee(coffee, guest))))

        case (`maxComplaintCount` -> _, Right(guest -> Left(Complaint(coffee)))) =>
          throw FrustratedException(coffee, guest)

        case (complaintCount -> _, Right(guest -> Left(Complaint(coffee)))) =>
          complaintCount + 1 -> Some(Right(Right(Barista.PrepareCoffee(coffee, guest))))

      }
      .collect { case (_, Some(response)) => response }

  def flowActor(
      coffeeHouse: ActorRef,
      barista: ActorRef,
      waiterFlow: Flow[Request[ActorRef], Response[ActorRef], NotUsed]
  )(implicit system: ActorSystem): ActorRef =
    system.actorOf(Props(new Actor {
      val (ref, fut) = Source
        .actorRef[Any](
          completionMatcher = PartialFunction.empty,
          failureMatcher = PartialFunction.empty,
          bufferSize = 0,
          overflowStrategy = OverflowStrategy.fail
        )
        .map[Request[ActorRef]] {
          case (sender: ActorRef, Barista.CoffeePrepared(coffee, guest: ActorRef)) =>
            Left(Barista.CoffeePrepared(coffee, guest))
          case (sender: ActorRef, serveCoffee: ServeCoffee) =>
            Right(sender -> Right(serveCoffee))
          case (sender: ActorRef, complaint: Complaint) =>
            Right(sender -> Left(complaint))
        }
        .via(waiterFlow)
        .toMat(Sink.foreach {
          case Left(guest -> coffeeServed) => guest ! coffeeServed
          case Right(Left(approveCoffee))  => coffeeHouse ! approveCoffee
          case Right(Right(prepareCoffee)) => barista ! prepareCoffee
        })(Keep.both)
        .run()

      fut.onComplete {
        case Success(Done) => self ! PoisonPill
        case Failure(ex)   => throw ex
      }(system.dispatcher)

      def receive: Receive = { msg => ref ! sender() -> msg }
    }))
}

class Waiter(coffeeHouse: ActorRef, barista: ActorRef, maxComplaintCount: Int) extends Actor {

  import Waiter._

  private var complaintCount = 0

  override def receive: Receive = {
    case ServeCoffee(coffee) =>
      coffeeHouse ! CoffeeHouse.ApproveCoffee(coffee, sender())
    case Barista.CoffeePrepared(coffee, guest: ActorRef) =>
      guest ! CoffeeServed(coffee)
    case Complaint(coffee) if complaintCount == maxComplaintCount =>
      throw FrustratedException(coffee, sender())
    case Complaint(coffee) =>
      complaintCount += 1
      barista ! Barista.PrepareCoffee(coffee, sender())
  }
}
