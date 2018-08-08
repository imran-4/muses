package de.tuberlin.dima.bdapro.muses.akka.publisher

import akka.actor.{Actor, ActorLogging}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Publish

class Publisher extends Actor with ActorLogging {
  val mediator = DistributedPubSub(context.system).mediator
  def receive = {
    case in: String => {
      log.info("Received String: {}", in)
      mediator ! Publish("content", in)
    }
    case in: Array[Byte] => {
      log.info("Received Byte array of length: {}", in.length)
      println("TIME IN PUBLISHER (WHEN PUBLISHING)" + System.currentTimeMillis())
      mediator ! Publish("content", in)
    }
  }
}