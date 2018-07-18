package de.tuberlin.dima.bdapro.muses.akka.main

import java.nio.file.Paths

import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{FileIO, Flow, GraphDSL, Keep, Sink, Source}
import jnr.ffi.annotations.{In, Out}

class Subscriber extends Actor with ActorLogging {
  import DistributedPubSubMediator.{Subscribe, SubscribeAck}
  val distributedPubSub = DistributedPubSub(context.system)
  val mediator = distributedPubSub.mediator
//  mediator ! Subscribe("content", self)
  implicit val materializer = ActorMaterializer()
  val sink = FileIO.toPath(Paths.get("hello1_2_3.txt"))

  Source
    .actorRef(2, OverflowStrategy.fail)
    .mapMaterializedValue {
      ref =>
        mediator ! Subscribe("content", self);
        self
    }.to(sink).run()

  //........
//  val source =
//    Source
//      .actorRef[Out](10, OverflowStrategy.dropHead)
//      .mapMaterializedValue { ref => mediator ! Subscribe("content", ref); ref }

//  val wrapWithPublish =
//    Flow[In].map(DistributedPubSubMediator.Publish("content", _))
//
//  val unsubscribe = DistributedPubSubMediator.Unsubscribe("content", mediator)
//
//  val toPubsub =
//    Sink.actorRef[DistributedPubSubMediator.Publish](mediator, unsubscribe)
//
//  Flow.fromSinkAndSource(wrapWithPublish to toPubsub, source)
  //............
//  Source
//    .actorRef(Int.MaxValue, OverflowStrategy.fail)
//    .mapMaterializedValue {
//      ref =>
//        println("><<<<<<<<<<>>???>>>><<<")
//        mediator ! DistributedPubSubMediator.Subscribe("content", self);
//        self
//      }.to(sink).run()

  def receive = {
    case s: String =>
      log.info("Received: {}", s)
    case SubscribeAck(Subscribe("content1", None, `self`)) =>
      log.info("subscribing")
  }
}