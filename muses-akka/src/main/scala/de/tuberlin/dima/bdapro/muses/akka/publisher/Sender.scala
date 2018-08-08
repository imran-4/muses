package de.tuberlin.dima.bdapro.muses.akka.publisher

import akka.actor.{Actor, ActorLogging}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import DistributedPubSubMediator.Send

class Sender extends Actor with ActorLogging {
  val mediator = DistributedPubSub(context.system).mediator

  def receive = {
    case in: String => {
      log.info("Received String: {}", in)
      mediator ! Send(msg = in, path="/user/subscriber", localAffinity = true)
    }
    case in: Array[Byte] => {
      log.info("Received String: {}", in)
      mediator ! Send(msg = in, path="/user/subscriber", localAffinity = true)
    }
  }
}