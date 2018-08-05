package de.tuberlin.dima.bdapro.muses.akka.publisher


import java.io.ByteArrayOutputStream

import akka.actor.{Actor, ActorLogging}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import DistributedPubSubMediator.Send
import DistributedPubSubMediator.Publish
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.ipc.message.{ArrowBlock, ArrowRecordBatch}

class Sender extends Actor with ActorLogging {
  val mediator = DistributedPubSub(context.system).mediator

  def receive = {
    case in: String => {
      log.info("Received String: {}", in)
      mediator ! Send(msg = in, path="/user/subscriber", localAffinity = true)
    }
    case in: IntVector => {
      log.info("Received IntVector: {}", in)
      mediator ! Send(msg = in, path="/user/subscriber", localAffinity = true)
    }
    case in: ArrowRecordBatch => {
      log.info("Received ArrowRecordBatch: {}", in)
      mediator ! Send(msg = in, path="/user/subscriber", localAffinity = true)
    }
    case in:ByteArrayOutputStream => {
      log.info("Received String: {}", in)
      mediator ! Send(msg = in, path="/user/subscriber", localAffinity = true)
    }
    case in: ArrowBlock => {
      log.info("Received String: {}", in)
      mediator ! Send(msg = in, path="/user/subscriber", localAffinity = true)
    }
    case in: Array[Byte] => {
      log.info("Received String: {}", in)
      mediator ! Send(msg = in, path="/user/subscriber", localAffinity = true)
    }
  }
}