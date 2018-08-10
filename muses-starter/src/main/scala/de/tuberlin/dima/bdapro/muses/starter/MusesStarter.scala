package de.tuberlin.dima.bdapro.muses.starter

import java.nio.file.{Files, Paths}

import akka.actor.Props
import de.tuberlin.dima.bdapro.muses.akka.main.MainPubSub
import de.tuberlin.dima.bdapro.muses.connector.arrow.writer.Writer

object MusesStarter {
  def main(args: Array[String]): Unit = {
//    System.out.println("Hello from Muses Start point")
//    val confFilePath = args(0).substring(args(0).indexOf(":") + 1)
//    System.out.println("Path: " + args(0))
//    Files.lines(Paths.get(confFilePath.trim)).forEach((x: String) => System.out.println(x))

    var rol = "sub"
    if (rol == "pub") {
      val writer = new Writer
      var (rs, cols) = writer.readDatabase()
      writer.write(rs ,cols)
      var schema = writer.getSchemaJson()
      var os = writer.getByteArrayOutputStream()
      var main = new MainPubSub
      main.createPubliser("publisher")
      main.publishSchema(schema)
      main.publishData(os.toByteArray)
      main.attachShutdownHook()
    } else {
      var main = new MainPubSub
      main.createSubscriber("subscriber")
      main.attachShutdownHook()
    }
  }
}