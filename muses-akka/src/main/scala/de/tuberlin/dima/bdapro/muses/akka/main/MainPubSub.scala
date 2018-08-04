package de.tuberlin.dima.bdapro.muses.akka.main

import java.io.ObjectOutputStream

import akka.actor.{ActorSystem, Deploy, Props}
import akka.cluster.Cluster
import akka.remote.serialization.ProtobufSerializer
import akka.stream.ActorMaterializer
import com.google.flatbuffers.FlatBufferBuilder
import de.tuberlin.dima.bdapro.muses.connector.Test
import org.apache.arrow.memory.{AllocationListener, RootAllocator}
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.ipc.message.ArrowMessage.ArrowMessageVisitor
import org.apache.arrow.vector.ipc.message.{ArrowRecordBatch, MessageSerializer}

import scala.concurrent.Await
import scala.concurrent.duration._

object MainPubSub //extends AkkaSpec with CompileOnlySpec
{
  def main(args: Array[String]): Unit = {
    val systemName = "MusesCluster1"

    implicit val system1 = ActorSystem(systemName)
    implicit val materializer = ActorMaterializer()
    val joinAddress = Cluster(system1).selfAddress
    Cluster(system1).join(joinAddress)
    println(">>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<")
    val publisher = system1.actorOf(Props[Publisher].withDeploy(Deploy.local), "publisher")
    Thread.sleep(5000)
    implicit val system2 = ActorSystem(systemName)
    Cluster(system2).join(joinAddress)
    val subscriber = system2.actorOf(Props[Subscriber].withDeploy(Deploy.local), "subscriber")
    Thread.sleep(5000)
    /*******************/
    sys.addShutdownHook({
      println("Terminating...")
      system1.terminate()
      Await.result(system1.whenTerminated, 120 seconds)
      println("Terminated... Bye")
      println(System.currentTimeMillis())
    })
    sys.addShutdownHook({
      println("Terminating...")
      system2.terminate()
      Await.result(system2.whenTerminated, 120 seconds)
      println("Terminated... Bye")
      println(System.currentTimeMillis())
    })
//    /*******************/

//    var args: String = "pub"
//    val systemName = "MusesCluster"
//    var joinAddress = null
//    if (args == "pub") {
//
//      implicit val system1 = ActorSystem(systemName)
//      implicit val materializer = ActorMaterializer()
//      val publisher = system1.actorOf(Props[Publisher], "publisher")
//


    ////////////////////////////////////////////////




    //////////////////////////////////////////////////
    var test = new Test()
    var batch = test.execute()
    import java.io.ByteArrayOutputStream
    import java.nio.channels.Channels

    import org.apache.arrow.vector.ipc.WriteChannel
    val out = new ByteArrayOutputStream


    var channel = new WriteChannel(Channels.newChannel(out))
    var block = MessageSerializer.serialize(channel, batch)


//    var ser: ProtobufSerializer = new ProtobufSerializer()
//    ser.toBinary(batch)



    println()
    publisher ! out.toByteArray



    //    } else {
//
//      implicit val system2 = ActorSystem(systemName)
//      implicit val materializer = ActorMaterializer()
//      val subscriber = system2.actorOf(Props[Subscriber], "subscriber")
//
//    }


    /*********************************************************************************/
    //================================================================================

//    //////?????????????
//    val roundRobinPool = RoundRobinPool(nrOfInstances = 2)
//    val router  = system1.actorOf(roundRobinPool.props(Props[Publisher]))
//    (1 to 100).foreach(i => router ! i)


    //val sink = FileIO.toPath(Paths.get("out155.txt"))
//    val t: RunnableGraph[NotUsed] = source.via(flow).to(Sink.ignore)
//    t.run()

//    val s1 = Source.actorRef[ByteString](Int.MaxValue, OverflowStrategy.fail)
//
//
//    val ref = Flow[ByteString]
//      .to(Sink.ignore)
//      .runWith(source)

//    publisher ! ref //! Weather("02139", 32.0, true)
/**************************************/
/*
.throttle(
      elements = 100,
      per = 1 second,
      maximumBurst = 100,
      mode = ThrottleMode.shaping
    )
*/

      /*.runForeach(x => {
      println("---------------+++++++++++++++++++++: " + )
      publisher ! x
    })*/
//
//
//    Thread.sleep(10000)
//    Await.ready(system1.whenTerminated, Duration(1, TimeUnit.MINUTES))
//    Await.ready(system1.whenTerminated, Duration(3, TimeUnit.MINUTES))
    /*****************************************/

//    val source1 = Source.actorRef[ByteString](Int.MaxValue, OverflowStrategy.backpressure)
    //Source.fromPublisher()

//    val ref = Flow[ByteString]
//      .to(Sink.ignore)
//      .runWith(source1)

//    publisher ! ByteString("02139", 32.0, true)

//    source.via(flow).to(Sink.ignore).run(publisher)


//    system1.stop(publisher)
//    system2.stop(subscriber)
    //system1.terminate()
    //system2.terminate()



//    publisher ! source
//    publisher !
//    source.via(flow)
//    Source.actorRef()


//    system1.terminate()
//    system2.terminate()
  }
}