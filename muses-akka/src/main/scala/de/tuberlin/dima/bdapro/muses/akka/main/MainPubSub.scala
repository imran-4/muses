package de.tuberlin.dima.bdapro.muses.akka.main

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.cluster.Cluster
import akka.routing.RoundRobinPool
import akka.stream.{ActorMaterializer, OverflowStrategy, ThrottleMode}
import akka.stream.scaladsl.{FileIO, Flow, Framing, Keep, RunnableGraph, Sink, Source}
import akka.util.ByteString

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object MainPubSub {
  def main(args: Array[String]): Unit = {
    val systemName = "MusesCluster"
    implicit val system1 = ActorSystem(systemName)
    implicit val materializer = ActorMaterializer()
    val joinAddress = Cluster(system1).selfAddress
    Cluster(system1).join(joinAddress)
    val publisher = system1.actorOf(Props[Publisher], "publisher")

//    implicit val system0 = ActorSystem(systemName)
//    Cluster(system0).join(joinAddress)
//    val subscriber0 = system0.actorOf(Props[Subscriber], "subscriber")
//    Thread.sleep(5000)

    Thread.sleep(5000)
    implicit val system2 = ActorSystem(systemName)
    Cluster(system2).join(joinAddress)
    val subscriber = system2.actorOf(Props[Subscriber], "subscriber")
    Thread.sleep(5000)

    def files = new java.io.File("/home/mi/test/").listFiles().map(_.getAbsolutePath).to[scala.collection.immutable.Iterable]
    val source = Source(files).flatMapConcat(filename => FileIO.fromPath(Paths.get(filename)))
    val flow = Framing.delimiter(ByteString("\n"), 256, allowTruncation = true)

    source.map(x => {
      publisher ! x.utf8String
    }).to(Sink.ignore).run()

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


  }
}