/** Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
  */

package com.lightbend.training.coffeehouse

import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props, SupervisorStrategy, Terminated}
import akka.routing.FromConfig
import scala.concurrent.duration.{MILLISECONDS => Millis, _}
import akka.stream.scaladsl.Flow
import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.GraphDSL
import akka.stream.FlowShape
import akka.stream.SourceShape
import akka.stream.Inlet
import akka.stream.UniformFanInShape
import akka.stream.scaladsl.Merge
import akka.stream.scaladsl.Broadcast
import akka.stream.ActorAttributes
import akka.stream.Supervision
import akka.actor.ActorSystem
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.RetryFlow
import akka.stream.scaladsl.RestartFlow
import akka.stream.RestartSettings
import scala.concurrent.duration.FiniteDuration

object CoffeeHouse {

  case class CreateGuest(favoriteCoffee: Coffee, caffeineLimit: Int)
  case class ApproveCoffee[K](coffee: Coffee, guest: K)
  case object GetStatus
  case class Status(guestCount: Int)

  def props(caffeineLimit: Int): Props =
    Props(new CoffeeHouse(caffeineLimit))

  /*
    creates one barista
    creates one waiter
      - restarts waiter when it crashes

    in:
      - CreateGuest (they're stopped when they crash)
      - ApproveCoffee (checks global caffeinelimit) (this comes from ... waiter ?)
      - Terminated guest: could be substream closing
      - GetStatus: could be sink with mutable reference?
    out:
      - Status
   */

  type GuestId = Int
  case object PleaseLeave
  type PleaseLeave = PleaseLeave.type
  case object KickedOutException extends IllegalStateException("Kicked out!")

  // Manages guests and their progress towards the house caffeine limit
  def guestBookFlow(
      caffeineLimit: Int
  ): Flow[ApproveCoffee[Int], Either[(Int, PleaseLeave), Barista.PrepareCoffee[Int]], NotUsed] =
    Flow[ApproveCoffee[GuestId]]
      .scan(
        Map.empty[GuestId, Int].withDefaultValue(0) ->
          Option.empty[Either[(GuestId, PleaseLeave), Barista.PrepareCoffee[GuestId]]]
      ) {
        case ((guestBook, _), ApproveCoffee(coffee, guestId)) if guestBook(guestId) < caffeineLimit =>
          (guestBook + (guestId -> (guestBook(guestId) + 1)), Some(Right(Barista.PrepareCoffee(coffee, guestId))))
        case ((guestBook, _), ApproveCoffee(_, guestId)) =>
          (guestBook - guestId, Some(Left(guestId -> PleaseLeave)))
      }
      .collect { case (_, Some(result)) => result }
      .async

  type GuestsInput = Either[CreateGuest, (Int, Either[PleaseLeave, Waiter.CoffeeServed])]

  // Manages the guests themselves
  def guestsFlow(
      guestFinishCoffeeDuration: FiniteDuration
  ): Flow[GuestsInput, (Status, Option[(GuestId, Guest.Response)]), NotUsed] =
    Flow[Either[CreateGuest, (GuestId, Either[PleaseLeave, Waiter.CoffeeServed])]]
      .scan((0, Option.empty[(GuestId, Either[CreateGuest, Either[PleaseLeave, Waiter.CoffeeServed]])])) {
        case ((nextGuestId, _), Left(createGuest)) =>
          (nextGuestId + 1, Some(nextGuestId -> Left(createGuest)))
        case ((nextGuestId, _), Right(guestId -> message)) =>
          (nextGuestId, Some(guestId -> Right(message)))
      }
      .collect { case (_, Some(either)) => either }
      .groupBy(100, _._1)
      .flatMapPrefix(1) {
        case Seq(guestId -> Left(CreateGuest(favoriteCoffee, guestCaffeineLimit))) =>
          Flow[(GuestId, Either[CreateGuest, Either[PleaseLeave, Waiter.CoffeeServed]])]
            .collect {
              case (guestId, Right(Left(PleaseLeave))) => throw KickedOutException
              case (guestId, Right(Right(coffee)))     => coffee
            }
            .via(Guest.flow(favoriteCoffee, guestFinishCoffeeDuration, guestCaffeineLimit))
            .map[Either[Int, (GuestId, Guest.Response)]](response => Right(guestId -> response))
            .merge(Source.single(Left(1)))
            .recoverWithRetries(
              -1,
              {
                case KickedOutException      => Source.single(Left(-1))
                case Guest.CaffeineException => Source.single(Left(-1))
              }
            )

        case _ =>
          Flow.fromSinkAndSource(Sink.ignore, Source.never)
      }
      .mergeSubstreams
      .scan((Status(0), Option.empty[(GuestId, Guest.Response)])) {
        case ((Status(count), _), Left(adjustment)) => (Status(count + adjustment), None)
        case ((Status(count), _), Right(response))  => (Status(count), Some(response))
      }

  def flow(caffeineLimit: Int) = //: Flow[CreateGuest, Status, NotUsed] =
    Flow.fromMaterializer[CreateGuest, Status, NotUsed] { (materializer, _) =>
      val config = materializer.system.settings.config

      val guests = guestsFlow(
        Duration(config.getDuration("coffee-house.guest.finish-coffee-duration", Millis), Millis)
      )

      val guestBook = guestBookFlow(caffeineLimit)

      // restart waiter when it crashes, but not before notifying the barista
      val waiter = RestartFlow.onFailuresWithBackoff(RestartSettings(0.seconds, 0.seconds, 0)) { () =>
        Waiter
          .flow[GuestId](config.getInt("coffee-house.waiter.max-complaint-count"))
          .recoverWithRetries(
            -1,
            { case ex @ Waiter.FrustratedException(coffee, guestId: GuestId) =>
              Source
                .single(Right(Right(Barista.PrepareCoffee(coffee, guestId))))
                .mergePreferred(Source.lazySingle(() => throw ex), false)
            }
          )
          .async
      }

      val barista = Barista
        .flow[Unit, GuestId](
          Duration(config.getDuration("coffee-house.barista.prepare-coffee-duration", Millis), Millis),
          config.getInt("coffee-house.barista.accuracy")
        )
        .async

      Flow.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
        import GraphDSL.Implicits._

        val createGuest = builder.add(Flow[CreateGuest])

        val guestsIn = builder.add(Merge[GuestsInput](3))
        val guestsOut = builder.add(Broadcast[(Status, Option[(GuestId, Guest.Response)])](2))
        val waiterIn = builder.add(Merge[Waiter.Request[GuestId]](2))
        val baristaIn = builder.add(Merge[Barista.PrepareCoffee[GuestId]](2))
        val waiterOut = builder.add(Broadcast[Waiter.Response[GuestId]](3))
        val guestBookOut = builder.add(Broadcast[Either[(Int, PleaseLeave), Barista.PrepareCoffee[Int]]](2))

        createGuest.map(Left(_)) ~> guestsIn ~> builder.add(guests) ~> guestsOut

        guestsOut.collect { case (_, Some(response)) => Right(response) } ~>
          waiterIn ~> builder.add(waiter) ~> waiterOut

        (baristaIn.out.map(() -> _) ~> builder.add(barista))
          .map { case (_, coffeePrepared) => Left(coffeePrepared) } ~> waiterIn

        waiterOut.collect { case Right(Left(approveCoffee)) => approveCoffee } ~> builder.add(guestBook) ~> guestBookOut
        waiterOut.collect { case Left(guestId -> coffeeServed) => Right(guestId -> Right(coffeeServed)) } ~> guestsIn
        waiterOut.collect { case Right(Right(prepareCoffee)) => prepareCoffee } ~> baristaIn

        guestBookOut.collect { case Left(guestId -> pleaseLeave) => Right(guestId -> Left(pleaseLeave)) } ~> guestsIn
        guestBookOut.collect { case Right(prepareCoffee) => prepareCoffee } ~> baristaIn

        FlowShape(createGuest.in, guestsOut.out(1).map(_._1).outlet)
      })
    }

  def flowActor(coffeeHouseFlow: Flow[CreateGuest, Status, _])(implicit system: ActorSystem): ActorRef =
    system.actorOf(Props(new Actor {
      var status = Status(0)
      val queue = Source
        .queue[CreateGuest](10)
        .via(coffeeHouseFlow)
        .to(Sink.foreach { status = _ })
        .run()

      def receive: Receive = {
        case createGuest: CreateGuest => queue.offer(createGuest)
        case GetStatus                => sender() ! status
      }

    }))

}

class CoffeeHouse(caffeineLimit: Int) extends Actor with ActorLogging {

  import CoffeeHouse._

  override val supervisorStrategy: SupervisorStrategy = {
    val decider: SupervisorStrategy.Decider = {
      case Guest.CaffeineException =>
        SupervisorStrategy.Stop
      case Waiter.FrustratedException(coffee, guest) =>
        barista.tell(Barista.PrepareCoffee(coffee, guest), sender())
        SupervisorStrategy.Restart
    }
    OneForOneStrategy()(decider orElse super.supervisorStrategy.decider)
  }

  private val baristaAccuracy =
    context.system.settings.config getInt "coffee-house.barista.accuracy"
  private val baristaPrepareCoffeeDuration =
    Duration(
      context.system.settings.config
        .getDuration("coffee-house.barista.prepare-coffee-duration", Millis),
      Millis
    )
  private val guestFinishCoffeeDuration =
    Duration(
      context.system.settings.config
        .getDuration("coffee-house.guest.finish-coffee-duration", Millis),
      Millis
    )
  private val waiterMaxComplaintCount =
    context.system.settings.config getInt "coffee-house.waiter.max-complaint-count"

  private val barista = createBarista()
  private val waiter = createWaiter()

  private var guestBook = Map.empty[ActorRef, Int] withDefaultValue 0

  log.debug("CoffeeHouse Open")

  override def receive: Receive = {
    case CreateGuest(favoriteCoffee, caffeineLimit) =>
      val guest: ActorRef = createGuest(favoriteCoffee, caffeineLimit)
      guestBook += guest -> 0
      log.info(s"Guest $guest added to guest book.")
      context.watch(guest)
    case ApproveCoffee(coffee, guest: ActorRef) if guestBook(guest) < caffeineLimit =>
      guestBook += guest -> (guestBook(guest) + 1)
      log.info(s"Guest $guest caffeine count incremented.")
      barista forward Barista.PrepareCoffee(coffee, guest)
    case ApproveCoffee(coffee, guest: ActorRef) =>
      log.info(s"Sorry, $guest, but you have reached your limit.")
      context.stop(guest)
    case Terminated(guest) =>
      log.info(s"Thanks, $guest, for being our guest!")
      guestBook -= guest
    case GetStatus =>
      sender() ! Status(context.children.size - 2)
  }

  protected def createBarista(): ActorRef =
    context.actorOf(
      FromConfig.props(Barista.props(baristaPrepareCoffeeDuration, baristaAccuracy)),
      "barista"
    )

  protected def createWaiter(): ActorRef =
    context.actorOf(Waiter.props(self, barista, waiterMaxComplaintCount), "waiter")

  protected def createGuest(favoriteCoffee: Coffee, caffeineLimit: Int): ActorRef =
    context.actorOf(Guest.props(waiter, favoriteCoffee, guestFinishCoffeeDuration, caffeineLimit))
}
