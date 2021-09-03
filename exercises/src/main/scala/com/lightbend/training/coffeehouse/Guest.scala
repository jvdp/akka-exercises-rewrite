/** Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
  */

package com.lightbend.training.coffeehouse

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Timers}

import scala.concurrent.duration.FiniteDuration
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import akka.NotUsed
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Sink
import akka.actor.ActorSystem

object Guest {

  case object CaffeineException extends IllegalStateException("Too much caffeine!")
  case object CoffeeFinished

  def props(
      waiter: ActorRef,
      favoriteCoffee: Coffee,
      finishCoffeeDuration: FiniteDuration,
      caffeineLimit: Int
  ): Props =
    Props(
      new Guest(waiter, favoriteCoffee, finishCoffeeDuration, caffeineLimit)
    )

  // *** streams ***

  type Response = Either[Waiter.Complaint, Waiter.ServeCoffee]

  def flow(
      favoriteCoffee: Coffee,
      finishCoffeeDuration: FiniteDuration,
      caffeineLimit: Int
  ): Flow[Coffee, Response, NotUsed] =
    Flow[Coffee]
      .log("guest-flow-request")
      .scan((0, Option.empty[Response])) {
        case ((coffeeCount, _), `favoriteCoffee`) =>
          coffeeCount + 1 -> Some(Right(Waiter.ServeCoffee(favoriteCoffee)))
        case ((coffeeCount, _), coffee) =>
          coffeeCount -> Some(Left(Waiter.Complaint(favoriteCoffee)))
      }
      .collect {
        case (coffeeCount, _) if coffeeCount >= caffeineLimit => throw CaffeineException
        case (_, Some(response))                              => response
      }
      .via(LogInfo("guest-flow-response"))
      .flatMapConcat {
        case response @ Left(_)  => Source.single(response)
        case response @ Right(_) => Source.single(response).delay(finishCoffeeDuration)
      }
      .merge(Source.single(Right(Waiter.ServeCoffee(favoriteCoffee))))

// *** compat ***

  def flowActor(
      waiter: ActorRef,
      guestFlow: Flow[Coffee, Response, NotUsed]
  )(implicit system: ActorSystem): ActorRef = {
    Source
      .actorRef[Any](
        completionMatcher = PartialFunction.empty,
        failureMatcher = PartialFunction.empty,
        bufferSize = 0,
        overflowStrategy = OverflowStrategy.fail
      )
      .map { case Waiter.CoffeeServed(coffee) => coffee }
      .via(guestFlow)
      .to(Sink.foreach[Response] {
        case Left(complaint) => waiter ! complaint
        case Right(more)     => waiter ! more
      })
      .run()
  }

}

class Guest(
    waiter: ActorRef,
    favoriteCoffee: Coffee,
    finishCoffeeDuration: FiniteDuration,
    caffeineLimit: Int
) extends Actor
    with ActorLogging
    with Timers {

  import Guest._

  private var coffeeCount = 0

  orderFavoriteCoffee()

  override def receive: Receive = {
    case Waiter.CoffeeServed(`favoriteCoffee`) =>
      coffeeCount += 1
      log.info("Enjoying my {} yummy {}!", coffeeCount, favoriteCoffee)
      timers.startSingleTimer(
        "coffee-finished",
        CoffeeFinished,
        finishCoffeeDuration
      )
    case Waiter.CoffeeServed(coffee) =>
      log.info("Expected a {}, but got a {}!", favoriteCoffee, coffee)
      waiter ! Waiter.Complaint(favoriteCoffee)
    case CoffeeFinished if coffeeCount >= caffeineLimit =>
      throw CaffeineException
    case CoffeeFinished =>
      orderFavoriteCoffee()
  }

  override def postStop(): Unit =
    log.info("Goodbye!")

  private def orderFavoriteCoffee(): Unit =
    waiter ! Waiter.ServeCoffee(favoriteCoffee)
}
