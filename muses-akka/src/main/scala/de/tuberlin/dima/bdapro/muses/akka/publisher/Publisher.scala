package de.tuberlin.dima.bdapro.muses.akka.publisher


import java.io.ByteArrayOutputStream

import akka.actor.{Actor, ActorLogging}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Publish
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.ipc.message.{ArrowBlock, ArrowRecordBatch}

class Publisher extends Actor with ActorLogging {
  val mediator = DistributedPubSub(context.system).mediator
  def receive = {
    case in: String => {
      log.info("Received String: {}", in)
      mediator ! Publish("content", in)
    }
    case in: IntVector => {
      log.info("Received IntVector: {}", in)
      mediator ! Publish("content", in)
    }
    case in: ArrowRecordBatch => {
      log.info("Received ArrowRecordBatch: {}", in)
      mediator ! Publish("content", in)
    }
    case in:ByteArrayOutputStream => {
      log.info("Received String: {}", in)
      mediator ! Publish("content", in)
    }
    case in: ArrowBlock => {
      log.info("Received String: {}", in)
      mediator ! Publish("content", in)
    }
    case in: Array[Byte] => {
      log.info("Received String: {}", in)
      println("TIME IN PUBLISHER (WHEN PUBLISHING)" + System.currentTimeMillis())

      mediator ! Publish("content", in)
      println("TIME IN PUBLISHER (WHEN PUBLISHED)" + System.currentTimeMillis())

    }
  }
}