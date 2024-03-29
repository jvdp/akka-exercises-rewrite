/** Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
  */

package com.lightbend.training.coffeehouse

import akka.actor.{Actor, ActorRef, Props, Stash, Timers}

import scala.concurrent.duration.FiniteDuration
import scala.util.Random
import akka.stream.scaladsl.Flow
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.OverflowStrategy
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.scaladsl.Sink

object Barista {

  case class PrepareCoffee[K](coffee: Coffee, guest: K)
  case class CoffeePrepared[K](coffee: Coffee, guest: K)

  def props(prepareCoffeeDuration: FiniteDuration, accuracy: Int): Props =
    Props(new Barista(prepareCoffeeDuration, accuracy))

  // *** streams ***

  def flow[T, K](
      prepareCoffeeDuration: FiniteDuration,
      accuracy: Int
  ): Flow[(T, PrepareCoffee[K]), (T, CoffeePrepared[K]), NotUsed] =
    Flow[(T, PrepareCoffee[K])]
      .map { case (waiter, PrepareCoffee(requested, guest)) =>
        val coffee = if (Random.nextInt(100) < accuracy) requested else Coffee.anyOther(requested)
        waiter -> CoffeePrepared(coffee, guest)
      }
      .delay(prepareCoffeeDuration)

  // *** compat ***

  def flowActor(
      baristaFlow: Flow[(ActorRef, PrepareCoffee[ActorRef]), (ActorRef, CoffeePrepared[ActorRef]), NotUsed]
  )(implicit system: ActorSystem): ActorRef =
    system.actorOf(Props(new Actor {
      val ref = Source
        .actorRef[(ActorRef, PrepareCoffee[ActorRef])](
          completionMatcher = PartialFunction.empty,
          failureMatcher = PartialFunction.empty,
          bufferSize = 0,
          overflowStrategy = OverflowStrategy.fail
        )
        .via(baristaFlow)
        .to(Sink.foreach { case (waiter, coffeePrepared) => waiter ! coffeePrepared })
        .run()

      def receive: Receive = { msg => ref ! (sender(), msg) }
    }))

}

class Barista(prepareCoffeeDuration: FiniteDuration, accuracy: Int) extends Actor with Stash with Timers {

  import Barista._

  def receive: Receive =
    ready

  private def ready: Receive = { case PrepareCoffee(coffee, guest) =>
    timers.startSingleTimer(
      "coffee-prepared",
      CoffeePrepared(pickCoffee(coffee), guest),
      prepareCoffeeDuration
    )
    context.become(busy(sender()))
  }

  private def busy(waiter: ActorRef): Receive = {
    case CoffeePrepared(coffee, guest: ActorRef) =>
      waiter ! CoffeePrepared(coffee, guest)
      unstashAll()
      context.become(ready)
    case _ =>
      stash()
  }

  private def pickCoffee(coffee: Coffee): Coffee =
    if (Random.nextInt(100) < accuracy)
      coffee
    else
      Coffee.anyOther(coffee)
}
