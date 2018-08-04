package de.tuberlin.dima.bdapro.muses.akka.main

import java.io.ByteArrayOutputStream
import java.nio.file.Paths

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{FileIO, Flow, GraphDSL, Keep, Sink, Source}
import DistributedPubSubMediator.{Publish, Put}
import org.apache.arrow.vector.{IntVector, VectorLoader, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.message.{ArrowBlock, ArrowRecordBatch}
import org.apache.arrow.vector.types.pojo.Schema

class Subscriber extends Actor with ActorLogging {
  import DistributedPubSubMediator.{Subscribe, SubscribeAck}
  val distributedPubSub = DistributedPubSub(context.system)
  val mediator = distributedPubSub.mediator
  mediator ! Subscribe.apply("content", self)

  def receive = {
    case s: String =>
      log.info("Received>>>: {}", s)
    case s: Int =>
      log.info("Received>>>: {}", s)
    case s: IntVector =>
      log.info("Received>>>"+ s.toString)
    case s: ArrowRecordBatch =>
      log.info("Received   BATCHE ++++ ???   >>>"+ s.toString)
    case in:ByteArrayOutputStream => {
      var out = in
      println("Received BATCH STREAM>>>>>>>>>:  " + out.toString)
      log.info("INFO:::::::?????????????////" + out.toString)
    }
    case in: Array[Byte] => {
      log.info("RECEIVED<<<<<<<<<<<<<<<<<<<<SUBSCRIBER>>>>>>>>>>>>>> ")
      import org.apache.arrow.memory.BufferAllocator
      import org.apache.arrow.memory.RootAllocator
      val alloc = new RootAllocator(Long.MaxValue)

      import org.apache.arrow.vector.ipc.ReadChannel
      import org.apache.arrow.vector.ipc.message.ArrowMessage
      import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
      import org.apache.arrow.vector.ipc.message.MessageSerializer
      import java.io.ByteArrayInputStream
      import java.nio.channels.Channels
      val inputStream = new ByteArrayInputStream(in)
      val channel = new ReadChannel(Channels.newChannel(inputStream))
      val deserialized = MessageSerializer.deserializeMessageBatch(channel, alloc)
      println("CLASS RECEIVED:>>>>>" + deserialized.getClass)

      var recordBatch = deserialized.asInstanceOf[ArrowRecordBatch]


      println("RECORD BATCH IN SUB:" + recordBatch.toString)

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
      var out = in
      log.info("RECEIVED<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>  " + out.toString)


}
    case SubscribeAck(Subscribe("content", None, `self`)) =>
      log.info("subscribing")
  }
}