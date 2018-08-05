package de.tuberlin.dima.bdapro.muses.akka.main

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels

import akka.actor.{ActorSystem, Deploy, Props}
import akka.cluster.Cluster
import akka.stream.ActorMaterializer
import de.tuberlin.dima.bdapro.muses.connector.Test
import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer

import scala.concurrent.Await
import scala.concurrent.duration._

object MainPubSub
{
  def main(args: Array[String]): Unit = {
    val systemName = "MusesCluster1"

    implicit val system1 = ActorSystem(systemName)
    implicit val materializer = ActorMaterializer()
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
//
//      implicit val system1 = ActorSystem(systemName)
//      implicit val materializer = ActorMaterializer()
//      val publisher = system1.actorOf(Props[Publisher], "publisher")

///////////////////////////////////////////////////
    var test = new Test()
    var batch = test.execute()
    val out = new ByteArrayOutputStream
    var channel = new WriteChannel(Channels.newChannel(out))
    var block = MessageSerializer.serialize(channel, batch)
    println("Starting to publish...")
    publisher ! out.toByteArray
    println("Published")
    //...........................................................
    sys.addShutdownHook({
      println("Terminating System1...")
      system1.terminate()
      Await.result(system1.whenTerminated, 120 seconds)
      println("Terminated System1... Bye")
      println(System.currentTimeMillis())
    })
    sys.addShutdownHook({
      println("Terminating System2...")
      system2.terminate()
      Await.result(system2.whenTerminated, 120 seconds)
      println("Terminated System2... Bye")
      println(System.currentTimeMillis())
    })
    //...........................................................
  }
}