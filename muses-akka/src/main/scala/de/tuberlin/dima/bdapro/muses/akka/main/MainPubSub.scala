package de.tuberlin.dima.bdapro.muses.akka.main

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels

import akka.actor.{ActorRef, ActorSystem, Deploy, Props}
import akka.cluster.Cluster
import akka.stream.ActorMaterializer
import com.google.flatbuffers.FlatBufferBuilder
import com.sun.xml.internal.ws.api.server.SDDocument.Schema
import de.tuberlin.dima.bdapro.muses.akka.publisher.Publisher
import de.tuberlin.dima.bdapro.muses.akka.subscriber.Subscriber
import de.tuberlin.dima.bdapro.muses.connector.Test
import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer

import scala.concurrent.Await
import scala.concurrent.duration._

object MainPubSub
{
  //TODO: UNCOMMENT IN REAL ENVIRONMENT
//  var systemName:String = "MusesCluster"
//  implicit val system = ActorSystem(systemName)
//  implicit val materializer = ActorMaterializer()
//
//  def startPublisherActor(systemName: String): ActorRef = {
//    val publisherActorName = "publiser"
//    val publisher = system.actorOf(Props[Publisher], publisherActorName)
//    return publisher
//  }
//
//  def startSubScriberActor(systemName: String): ActorRef = {
//    val subscriberActorName = "subscriber"
//    val subscriber = system.actorOf(Props[Subscriber], subscriberActorName)
//    return subscriber
//  }

  def addShutDownHook(system: ActorSystem): Unit = {
    sys.addShutdownHook({
      system.terminate()
      Await.result(system.whenTerminated, 120 seconds)
      println("Terminated System.")
      println(System.currentTimeMillis())
    })
  }

  def publish(publisherActor: ActorRef, batchByteArray: Array[Byte]): Unit = {
    publisherActor ! batchByteArray
  }

  def main(args: Array[String]): Unit = {
    val systemName = "MusesCluster"
    implicit val system1 = ActorSystem(systemName)
    implicit val materializer1 = ActorMaterializer()
    val joinAddress = Cluster(system1).selfAddress
    Cluster(system1).join(joinAddress)
    val publisher = system1.actorOf(Props[Publisher].withDeploy(Deploy.local), "publisher")
    println("System1 Joined Cluster")

    Thread.sleep(5000)
    implicit val system2 = ActorSystem(systemName)
    Cluster(system2).join(joinAddress)
    val subscriber = system2.actorOf(Props[Subscriber].withDeploy(Deploy.local), "subscriber")
    println("System2 Joined Cluster")
    Thread.sleep(5000)
//////////////////////////////////////////////////
//    var args: String = "pub"
//    val systemName = "MusesCluster"
//    var joinAddress = null
//    if (args == "pub") {
//      implicit val system1 = ActorSystem(systemName)
//      implicit val materializer = ActorMaterializer()
//      val publisher = system1.actorOf(Props[Publisher], "publisher")
//    }
///////////////////////////////////////////////////
    var test = new Test()
    var (batch, schema) = test.execute()

    val out = new ByteArrayOutputStream
    var channel = new WriteChannel(Channels.newChannel(out))
    var block = MessageSerializer.serialize(channel, batch)

    println("Starting to publish...")
    println("TIME (WHEN PUBLISHING)" + System.currentTimeMillis())

    publisher ! schema
    publisher ! out.toByteArray

    println("Published")
    //...........................................................
    addShutDownHook(system1)
    addShutDownHook(system2)
    //...........................................................
  }
}