package com.lightbend.training.coffeehouse

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.FlowOps
import akka.stream.Attributes
import akka.stream.Attributes.LogLevels

object LogInfo {
  def apply[T](name: String): Flow[T, T, NotUsed] =
    Flow[T]
      .log(name)
      .addAttributes(Attributes.logLevels(Attributes.logLevelInfo))
}
