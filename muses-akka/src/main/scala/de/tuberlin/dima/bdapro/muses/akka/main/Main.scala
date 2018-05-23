package de.tuberlin.dima.bdapro.muses.akka.main


// AKKA Stream Example (not to add in github)

import akka.stream._
import akka.stream.scaladsl._

import akka.actor.ActorSystem


object Main extends App {
    implicit val system = ActorSystem()
    implicit val mat = ActorMaterializer()
    val source = Source(0  to 2000)
    val flow = Flow[Int].map(_.toString)
    val sink = Sink.foreach[String](println(_))
    val runnable = source.via(flow).to(sink)
    runnable.run()
}