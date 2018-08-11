package de.tuberlin.dima.bdapro.muses.akka.main

import akka.actor.{ActorRef, ActorSystem, Address, Deploy, Props}
import akka.cluster.Cluster
import akka.stream.ActorMaterializer
import de.tuberlin.dima.bdapro.muses.akka.publisher.Publisher
import de.tuberlin.dima.bdapro.muses.akka.subscriber.Subscriber

import scala.concurrent.Await
import scala.concurrent.duration._

class MainPubSub
{
  private var systemName:String = "MusesCluster"
  private implicit val system = ActorSystem(systemName)
  private implicit val materializer = ActorMaterializer()
  private var actorRef: ActorRef = null

  def createPubliser(actorRefName: String): Unit = {
    this.actorRef = this.system.actorOf(Props[Publisher].withDeploy(Deploy.local), actorRefName)

  }

  def createSubscriber(actorRefName: String): Unit = {
    this.actorRef = this.system.actorOf(Props[Subscriber].withDeploy(Deploy.local), actorRefName)
  }

  def attachShutdownHook(): Unit = {
    sys.addShutdownHook({
      system.terminate()
      Await.result(system.whenTerminated, 120 seconds)
      println("Terminated System.")
      println(System.currentTimeMillis())
    })
  }

  def publishSchema(schema: String): Unit = {
    this.actorRef ! schema
  }

  def publishData(batchByteArray: Array[Byte]): Unit = {
    this.actorRef ! batchByteArray
  }
}