package de.tuberlin.dima.bdapro.muses.akka.main

import java.nio.ByteOrder
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink

import scala.util._
import akka._
import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import akka.util._
import akka.stream.stage._


object TCPServer {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("MusesTCPServer")
    implicit val materializer = ActorMaterializer()


    if (args.isEmpty) {
      val address = "127.0.0.1"
      val port = 10001
      createServer(address, port)
    } else {
      val address = args(0)
      val port = args(1).toInt
      createServer(address, port)
    }
  }

  def createServer(address: String, port: Int)(implicit system: ActorSystem, materializer: Materializer): Unit = {
    import system.dispatcher

    // bind tcp connections
    val incomingConnections = Tcp().bind(address, port)

    val sink = FileIO.toPath(Paths.get("/home/mi/hello_00001.txt"))
    // create sink
    val connectionHandler = Sink.foreach[Tcp.IncomingConnection] {
      conn =>
        system.log.debug(s"Incoming connection from: ${conn.remoteAddress}")
//        conn.handleWith(serverLogic(conn))

        conn.handleWith(
        Flow[ByteString]
//          .via(Framing.lengthField(4, 0, Int.MaxValue, ByteOrder.BIG_ENDIAN))
          .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 256, allowTruncation = true))
          .map(msg => msg)
          .alsoTo(sink)
          .map(_ => ByteString.empty)
          .filter(_ => false) // Prevents sending anything back
//          .via(closeConnection)
      )
    }

    val binding = incomingConnections.to(connectionHandler).run()
    binding.onComplete({
      case Success(binding) =>
        println(s"Server started on address: ${binding.localAddress}")
      case Failure(error) =>
        println(s"Server could not be bound to $address:$port: ${error.getMessage}")
    })
  }

  val closeConnection = new GraphStage[FlowShape[String, String]] {
    val in = Inlet[String]("closeConnection.in")
    val out = Outlet[String]("closeConnection.out")

    override val shape = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) {
     setHandler(in, new InHandler {
       override def onPush(): Unit = grab(in) match {
         case "q" =>
           push(out, "BYE")
           completeStage()
         case msg =>

           push(out, s"Server responds to message: $msg\n")
       }
     })
      setHandler(out, new OutHandler {
        override def onPull(): Unit = pull(in)
      })
    }
  }
//
//  def serverLogic(conn: Tcp.IncomingConnection)(implicit system: ActorSystem): Flow[ByteString, ByteString, NotUsed]= Flow.fromGraph(GraphDSL.create() { implicit b ⇒
//    import GraphDSL.Implicits._
//
//    val welcome = Source.single(ByteString(s"Welcome port ${conn.remoteAddress}!\n"))
//    val logic = b.add(Flow[ByteString]
//      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 256, allowTruncation = true))
//      .map(_.utf8String)
//      .map { msg ⇒ system.log.debug(s"Server received: $msg"); msg }
//      .via(closeConnection)
//      .map(ByteString(_)))
//
//
//    val concat = b.add(Concat[ByteString]())
//    welcome ~> concat.in(0)
//    logic.outlet ~> concat.in(1)
//
//    FlowShape(logic.in, concat.out)
//  })

}