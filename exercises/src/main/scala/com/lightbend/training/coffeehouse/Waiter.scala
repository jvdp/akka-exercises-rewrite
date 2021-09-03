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
  case class FrustratedException(coffee: Coffee, guest: ActorRef) extends IllegalStateException("Too many complaints!")

  def props(coffeeHouse: ActorRef, barista: ActorRef, maxComplaintCount: Int): Props =
    Props(new Waiter(coffeeHouse, barista, maxComplaintCount))

  type Request = Either[ServeCoffee, Either[Barista.CoffeePrepared, Complaint]]
  type Response = Either[CoffeeHouse.ApproveCoffee, Either[(ActorRef, CoffeeServed), Barista.PrepareCoffee]]

  def flow(maxComplaintCount: Int): Flow[(ActorRef, Request), Response, NotUsed] =
    Flow[(ActorRef, Request)]
      .scan((0, Option.empty[Response])) {
        case (complaintCount -> _, sender -> Left(ServeCoffee(coffee))) =>
          complaintCount -> Some(Left(CoffeeHouse.ApproveCoffee(coffee, sender)))

        case (complaintCount -> _, _ -> Right(Left((Barista.CoffeePrepared(coffee, guest))))) =>
          complaintCount -> Some(Right(Left(guest -> CoffeeServed(coffee))))

        case (`maxComplaintCount` -> _, sender -> Right(Right(Complaint(coffee)))) =>
          throw FrustratedException(coffee, sender)

        case (complaintCount -> _, sender -> Right(Right(Complaint(coffee)))) =>
          complaintCount + 1 -> Some(Right(Right(Barista.PrepareCoffee(coffee, sender))))

      }
      .collect { case (_, Some(response)) => response }

  def flowActor(
      coffeeHouse: ActorRef,
      barista: ActorRef,
      waiterFlow: Flow[(ActorRef, Request), Response, NotUsed]
  )(implicit system: ActorSystem): ActorRef =
    system.actorOf(Props(new Actor {
      val (ref, fut) = Source
        .actorRef[Any](
          completionMatcher = PartialFunction.empty,
          failureMatcher = PartialFunction.empty,
          bufferSize = 0,
          overflowStrategy = OverflowStrategy.fail
        )
        .map {
          case (sender: ActorRef, serveCoffee: ServeCoffee)               => sender -> Left(serveCoffee)
          case (sender: ActorRef, coffeePrepared: Barista.CoffeePrepared) => sender -> Right(Left(coffeePrepared))
          case (sender: ActorRef, complaint: Complaint)                   => sender -> Right(Right(complaint))
        }
        .via(waiterFlow)
        .toMat(Sink.foreach {
          case Left(approveCoffee)                => coffeeHouse ! approveCoffee
          case Right(Left(guest -> coffeeServed)) => guest ! coffeeServed
          case Right(Right(prepareCoffee))        => barista ! prepareCoffee
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
    case Barista.CoffeePrepared(coffee, guest) =>
      guest ! CoffeeServed(coffee)
    case Complaint(coffee) if complaintCount == maxComplaintCount =>
      throw FrustratedException(coffee, sender())
    case Complaint(coffee) =>
      complaintCount += 1
      barista ! Barista.PrepareCoffee(coffee, sender())
  }
}
