package de.tuberlin.dima.bdapro.muses.akka.subscriber

import java.io.ByteArrayOutputStream

import akka.actor.{Actor, ActorLogging}
import akka.cluster.pubsub.DistributedPubSubMediator.{Subscribe, SubscribeAck}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import akka.stream.actor.ActorSubscriberMessage.OnComplete
import de.tuberlin.dima.bdapro.muses.connector.SubscribedStreamReader
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.ipc.message.{ArrowBlock, ArrowRecordBatch}

class Subscriber extends Actor with ActorLogging {
  var ssR = new SubscribedStreamReader()
  val distributedPubSub = DistributedPubSub(context.system)
  val mediator = distributedPubSub.mediator
  mediator ! Subscribe("content", self)
  def receive = {
    case in: String => {
      ssR.createSchema(in)
      log.info("Received String: {}", in)
    }
    case in: Int => {
      log.info("Received Int: {}", in)
    }
    case in: IntVector => {
      log.info("Received IntVector: {}", in.toString)
    }
    case in: ArrowRecordBatch => {
      log.info("Received ArrowRecordBatch: {}", in.toString)
    }
    case in:ByteArrayOutputStream => {
      log.info("Received ArrowRecordBatch: {}", in.toString)
    }
    case in: Array[Byte] => {
      println("TIME IN SUBSCRIBER (WHEN SUBSCRIBED)" + System.currentTimeMillis())
      log.info("Received Array[Byte]: {}", in.toString)
      ssR.loadVector(in)
      println("TIME IN SUBSCRIBER (WHEN SUBSCRIBER COMPLETED)" + System.currentTimeMillis())

    }
    case in: ArrowBlock => {
      log.info("Received ArrowBlock: {}", in.toString)
    }
    case SubscribeAck(Subscribe("content", None, `self`)) =>
      log.info("SubscribeAck: Subscription has been acknowledged.")
    case OnComplete =>
      log.info("Consumption of the data has been completed!")
      context.stop(self)
  }
}