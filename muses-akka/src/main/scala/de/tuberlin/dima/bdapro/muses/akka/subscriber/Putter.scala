package de.tuberlin.dima.bdapro.muses.akka.subscriber

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.channels.Channels

import akka.actor.{Actor, ActorLogging}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Put, Send, Subscribe, SubscribeAck}
import akka.stream.actor.ActorSubscriberMessage.OnComplete
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.message.{ArrowBlock, ArrowRecordBatch, MessageSerializer}

class Putter extends Actor with ActorLogging {
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! Put(self)

  def receive = {
    case in: String => {
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
      log.info("Received Array[Byte]: {}", in.toString)
      val alloc = new RootAllocator(Long.MaxValue)
      val inputStream = new ByteArrayInputStream(in)
      val channel = new ReadChannel(Channels.newChannel(inputStream))
      val deserialized = MessageSerializer.deserializeMessageBatch(channel, alloc)
      println("Deserialized Class: {}", deserialized.getClass)
      var recordBatch = deserialized.asInstanceOf[ArrowRecordBatch]
      println("RecordBatch: {}", recordBatch.toString)
      //      val finalVectorsAllocator = alloc.newChildAllocator("final vectors", 0, Integer.MAX_VALUE)
      //      val newRoot = VectorSchemaRoot.create(schema, finalVectorsAllocator)
      //              val vectorLoader = new VectorLoader(newRoot)
      //              vectorLoader.load(recordBatch)
      //              val intReader = newRoot.getVector("emp_no").getReader
      //              //        val bigIntReader = newRoot.getVector("bigInt").getReader
      //              var i = 0
      //              var count = 20
      //              while (i < count) {
      //                intReader.setPosition(i)
      //                println(intReader.readInteger().intValue())
      //                i += 1
      //              }
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