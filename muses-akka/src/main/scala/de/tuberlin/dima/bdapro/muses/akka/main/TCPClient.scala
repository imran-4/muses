package de.tuberlin.dima.bdapro.muses.akka.main

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{FileIO, Flow, Framing, Sink, Source, Tcp}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import de.tuberlin.dima.bdapro.muses.connector.Test


import scala.util.{Failure, Success}

object TCPClient {
  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("MusesTCPClient")
    implicit val materializer = ActorMaterializer()

    if (args.isEmpty) {
      val address = "127.0.0.1"
      val port = 10001
      createClient(address, port)
    } else {
      val address = args(0)
      val port = args(1).toInt
      createClient(address, port)
    }
  }

  def closeClient = new GraphStage[FlowShape[String, String]] {
    val in = Inlet[String]("closeClient.in")
    val out = Outlet[String]("closeClient.out")

    override val shape = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      setHandler(in, new InHandler {
        override def onPush(): Unit = grab(in) match {
          case "BYE" =>
            println("Connection closed")
            completeStage()
          case msg =>
            println(msg)
            push(out, msg)
        }
      })
      setHandler(out, new OutHandler {
        override def onPull(): Unit = pull(in)
      })
    }
  }

  def createClient(address: String, port: Int)(implicit system:ActorSystem, materializer: Materializer): Unit = {
    import system.dispatcher

    //create outgoing connection with address and port
    val connection = Tcp().outgoingConnection(address, port)

//    val flow = Flow[ByteString]
//      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 256, allowTruncation = true))
//      .map(_.utf8String)
//      .via(closeClient)
//      .map(_ => StdIn.readLine("> "))
//      .map(_+"\n")
//      .map(ByteString(_))

//    connection.join(flow).run()
    ///////////////////////////////////////

    // craete source and flow
    def files = new java.io.File("/home/mi/test/").listFiles().map(_.getAbsolutePath).to[scala.collection.immutable.Iterable]
    val source = Source(files).flatMapConcat(filename => FileIO.fromPath(Paths.get(filename)))
    val flow = Framing.delimiter(ByteString("\n"), 256, allowTruncation = true)

    var example= new Test()
//    Source.fromIterator(example.execute())
    Source.fromIterator(example.execute()).

    //here multiple flows can be created (if any transformations are required) ....

    //    val sink = FileIO.toPath(Paths.get("out15.txt"))
    //    val graph = source.via(flow).to(sink)
    //
    //    graph.run()

    val result = source.via(flow).via(Tcp().outgoingConnection(address, port)).
            runFold(ByteString.empty) { (acc, in) ⇒ acc ++ in }

    result.onComplete {
      case Success(successResult) =>
        println(s"Result: " + successResult.utf8String)
        println("Closing client")
        system.terminate()
      case Failure(e) =>
        println("Failed: " + e.getMessage)
        system.terminate()
    }
    ///////////////////////////////////////////
  }
}