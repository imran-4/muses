package de.tuberlin.dima.bdapro.muses.akka.main

import java.io.{ByteArrayOutputStream, NotSerializableException}

import akka.actor.Actor
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.ipc.message.{ArrowBlock, ArrowRecordBatch}

class Publisher extends Actor {
  import DistributedPubSubMediator.Publish
  val mediator = DistributedPubSub(context.system).mediator
  def receive = {
    case in: String => {
      val out = in
      println(s"Received>>>>>>>> '$in', transformed to '$out'.")
      mediator ! Publish("content", out)
    }
    case in: IntVector => {
      var out = in
      println("Received>>>>>>>>>:  " + out.toString)
      mediator ! Publish("content", out)
    }
    case in: ArrowRecordBatch => {
      var out = in
      println("Received BATCH>>>>>>>>>:  " + out.toString)

      mediator ! Publish("content", out)
    }
    case in:ByteArrayOutputStream => {
      var out = in
      println("Received BATCH STREAM>>>>>>>>>:  " + out.toString)

      mediator ! Publish("content", out)
    }
    case in: ArrowBlock => {
      var out = in
      println("RECEIVED<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>  " + out.toString)
      mediator ! Publish("content", out)
    }
    case in: Array[Byte] => {
      var out = in
      println("RECEIVED<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>  " + out.toString)
      mediator ! Publish("content", out)
    }
    case e: NotSerializableException ⇒ {
      println("NOT SERIALIZABLE>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + e.getMessage)
    }
  }
}