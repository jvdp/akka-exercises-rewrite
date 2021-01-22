/**
 * Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
 */

package com.lightbend.training.coffeehouse

import akka.testkit.{ EventFilter, TestProbe }

class CoffeeHouseSpec extends BaseAkkaSpec {

  "Creating CoffeeHouse" should {
    "result in logging a status message at debug" in {
      EventFilter.debug(pattern = ".*[Oo]pen.*", occurrences = 1) intercept {
        system.actorOf(CoffeeHouse.props)
      }
    }
  }

  "Sending CreateGuest to CoffeeHouse" should {
    "result in creating a Guest" in {
      val coffeeHouse = system.actorOf(CoffeeHouse.props, "create-guest")
      coffeeHouse ! CoffeeHouse.CreateGuest
      TestProbe().expectActor("/user/create-guest/$*")
    }
  }
}
