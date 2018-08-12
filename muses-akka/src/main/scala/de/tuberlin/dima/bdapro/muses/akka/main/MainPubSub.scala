package de.tuberlin.dima.bdapro.muses.akka.main

import java.io.File

import akka.actor.{ActorRef, ActorSystem, Address, Deploy, Props}
import akka.cluster.Cluster
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import de.tuberlin.dima.bdapro.muses.akka.publisher.Publisher
import de.tuberlin.dima.bdapro.muses.akka.subscriber.Subscriber

import scala.concurrent.Await
import scala.concurrent.duration._

class MainPubSub
{
  private var systemName:String = "MusesCluster"
  private implicit var system:ActorSystem = null //ActorSystem(systemName, this.config)
  private implicit var materializer:ActorMaterializer = null //ActorMaterializer()
  private var actorRef: ActorRef = null

  private var config: Config = null

  def createActorSystem(): Unit = {
    this.system = ActorSystem(systemName, this.config)
    this.materializer = ActorMaterializer()
  }

  def loadConfiguration(file: File): Unit = {
    var config = ConfigFactory.parseFile(file)
    this.config = ConfigFactory.load(config)
  }

  def createPubliser(actorRefName: String): Unit = {
    this.actorRef = this.system.actorOf(Props[Publisher], actorRefName)

  }

  def createSubscriber(actorRefName: String): Unit = {
    this.actorRef = this.system.actorOf(Props[Subscriber], actorRefName)
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