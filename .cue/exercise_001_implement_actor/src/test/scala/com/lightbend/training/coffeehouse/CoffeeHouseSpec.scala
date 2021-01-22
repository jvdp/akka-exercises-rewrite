/**
 * Copyright © 2014 - 2020 Lightbend, Inc. All rights reserved. [http://www.lightbend.com]
 */

package com.lightbend.training.coffeehouse

import akka.actor.Props
import akka.testkit.EventFilter

class CoffeeHouseSpec extends BaseAkkaSpec {

  "Sending a message to CoffeeHouse" should {
    "result in logging a 'coffee brewing' message at info" in {
      val coffeeHouse = system.actorOf(Props(new CoffeeHouse))
      EventFilter.info(source = coffeeHouse.path.toString, pattern = ".*[Cc]offee.*", occurrences = 1) intercept {
        coffeeHouse ! "Brew Coffee"
      }
    }
  }
}
