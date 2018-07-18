package de.tuberlin.dima.bdapro.muses.akka.main

import akka.actor.{Actor}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}

class Publisher extends Actor {
  import DistributedPubSubMediator.Publish
  val mediator = DistributedPubSub(context.system).mediator
  def receive = {
    case in: String => {
      val out = in
      println(s"Received '$in', transformed to '$out'.")
      mediator ! Publish("content", out)
    }
  }
}