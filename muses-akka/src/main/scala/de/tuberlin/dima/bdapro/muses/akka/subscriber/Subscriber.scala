package de.tuberlin.dima.bdapro.muses.akka.subscriber

import akka.actor.{Actor, ActorLogging}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Subscribe, SubscribeAck}
import akka.stream.actor.ActorSubscriberMessage.OnComplete
import de.tuberlin.dima.bdapro.muses.connector.arrow.reader.Reader

class Subscriber extends Actor with ActorLogging {
  var reader = new Reader()
  val distributedPubSub = DistributedPubSub(context.system)
  val mediator = distributedPubSub.mediator
  mediator ! Subscribe("content", self)
  def receive = {
    case in: String => {
      reader.createSchema(in)
      log.info("Received String: {}", in)
    }
    case in: Array[Byte] => {
      println("TIME IN SUBSCRIBER (WHEN SUBSCRIBED)" + System.currentTimeMillis())
      log.info("Received Array[Byte]: {}", in.toString)
      reader.read(in)
      println("TIME IN SUBSCRIBER (WHEN SUBSCRIBER COMPLETED)" + System.currentTimeMillis())
    }
    case SubscribeAck(Subscribe("content", None, `self`)) =>
      log.info("SubscribeAck: Subscription has been acknowledged.")
    case OnComplete =>
      log.info("Consumption of the data has been completed!")
      context.stop(self)
  }
}