package de.tuberlin.dima.bdapro.muses.starter

import java.io.{File, IOException}
import java.net.InetAddress
import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.{Files, Paths}
import java.util

import com.google.gson.{JsonObject, JsonParser}
import de.tuberlin.dima.bdapro.muses.akka.main.MainPubSub
import de.tuberlin.dima.bdapro.muses.connector.arrow.writer.Writer
import de.tuberlin.dima.bdapro.muses.connector.rdbms.connectionmanager.JDBCDriversInfo

object MusesStarter {

//  def jsonStringToMap(jsonString: String): Map[String, Any] = {
//    implicit val formats = org.json4s.DefaultFormats
//
//    parse(jsonString).extract[Map[String, Any]]
//  }

  def jsonStringToMap(jsonString: String): JsonObject = {
    val jsonObject = new JsonParser().parse(jsonString).getAsJsonObject
    return jsonObject
  }

  @throws[IOException]
  def readFile(path: String, encoding: Charset): String = {
    val encoded = Files.readAllBytes(Paths.get(path))
    new String(encoded, encoding)
  }

  def main(args: Array[String]): Unit = {

    var akkaConfigFilePath: String = args(1)

    println("JSON CONTENT: " + args(0))

    val configFilePath = args(0).trim
    val content: String = readFile(configFilePath, StandardCharsets.UTF_8)
    println("JSON CONTENT: " + content)
    var configurations = jsonStringToMap(content)

    /////////////////////
    //PUBLISHERS
    var publishers:util.ArrayList[Publisher] = new util.ArrayList[Publisher]()
    var numberOfPublishers = configurations.get("publishers").getAsJsonArray.size()
    for (i <- 0 until numberOfPublishers) {
      var publisherJsonObject = configurations.get("publishers").getAsJsonArray.get(0).getAsJsonObject
      var publisher: Publisher = new Publisher
      publisher.ip = publisherJsonObject.get("ip").getAsString
      publisher.port = publisherJsonObject.get("port").getAsInt
      publisher.actorName = publisherJsonObject.get("actorname").getAsString

      var numberOfSubscribers = publisherJsonObject.get("subscriberspath").getAsJsonArray.size()
      var subscribers:util.List[String] = new util.ArrayList[String]()
      for (j <- 0 until numberOfSubscribers) {
        var subscriber: String = publisherJsonObject.get("subscriberspath").getAsJsonArray.get(i).getAsString
        subscribers.add(subscriber)
      }
      publisher.subscriberPath = subscribers.toArray(new Array[String](0))

      val dbs = new util.ArrayList[DataSource]
      val dataSourcesLength = publisherJsonObject.get("dbs").getAsJsonArray.size()
      for (j <- 0 until dataSourcesLength) {

        var dbJsonObject = publisherJsonObject.get("dbs").getAsJsonArray.get(j).getAsJsonObject
        var ds = new DataSource
        ds.dataSourceType = DataSourceTypes.withName(dbJsonObject.get("type").getAsString.toUpperCase)

        var dbPropertiesJsonObject = dbJsonObject.get("properties").getAsJsonObject
        ds.properties = ds.dataSourceType match {
          case DataSourceTypes.RDBMS => {
            var rdbmsProp = new RDBMSDataSourceProperties
            rdbmsProp.driver = dbPropertiesJsonObject.get("driver").getAsString
            rdbmsProp.url = dbPropertiesJsonObject.get("url").getAsString
            rdbmsProp.userName = dbPropertiesJsonObject.get("username").getAsString
            rdbmsProp.password = dbPropertiesJsonObject.get("password").getAsString
            rdbmsProp.query = dbPropertiesJsonObject.get("query").getAsString
            rdbmsProp.partitionKey = dbPropertiesJsonObject.get("partitionkey").getAsString
            rdbmsProp.numberOfPartitions = dbPropertiesJsonObject.get("totalpartitions").getAsInt

            rdbmsProp
          }
          case _ => throw new Exception("The type is not supported yet.")
        }
        dbs.add(ds)
      }
      publisher.dataSources = dbs.toArray(new Array[DataSourceBase](0))

      publishers.add(publisher)
    }

    /////////////////////
    //SUBSCRIBERS
    var subscribers:util.ArrayList[Subscriber] = new util.ArrayList[Subscriber]()
    var numberOfSubscribers = configurations.get("subscribers").getAsJsonArray.size()

    for (i <- 0 until numberOfSubscribers) {
      var subscriberJsonObject = configurations.get("subscribers").getAsJsonArray.get(0).getAsJsonObject
      var subscriber: Subscriber = new Subscriber
      subscriber.ip = subscriberJsonObject.get("ip").getAsString
      subscriber.port = subscriberJsonObject.get("port").getAsInt
      subscriber.actorName = subscriberJsonObject.get("actorname").getAsString

      val dbs = new util.ArrayList[DataSource]
      val dataSourcesLength = subscriberJsonObject.get("dbs").getAsJsonArray.size()
      for (j <- 0 until dataSourcesLength) {

        var dbJsonObject = subscriberJsonObject.get("dbs").getAsJsonArray.get(j).getAsJsonObject
        var ds = new DataSource
        ds.dataSourceType = DataSourceTypes.withName(dbJsonObject.get("type").getAsString.toUpperCase)

        var dbPropertiesJsonObject = dbJsonObject.get("properties").getAsJsonObject
        ds.properties = ds.dataSourceType match {
          case DataSourceTypes.RDBMS => {
            var rdbmsProp = new RDBMSDataSourceProperties
            rdbmsProp.driver = dbPropertiesJsonObject.get("driver").getAsString
            rdbmsProp.url = dbPropertiesJsonObject.get("url").getAsString
            rdbmsProp.userName = dbPropertiesJsonObject.get("username").getAsString
            rdbmsProp.password = dbPropertiesJsonObject.get("password").getAsString
            rdbmsProp.query = dbPropertiesJsonObject.get("query").getAsString
            rdbmsProp.partitionKey = dbPropertiesJsonObject.get("partitionkey").getAsString
            rdbmsProp.numberOfPartitions = dbPropertiesJsonObject.get("totalpartitions").getAsInt

            rdbmsProp
          }
          case DataSourceTypes.FILE => {
            var fileDBProp = new FileDataSourceProperties
            fileDBProp.filePath = dbPropertiesJsonObject.get("path").getAsString

            fileDBProp
          }
          case _ => throw new Exception("The type is not supported yet.")
        }
        dbs.add(ds)
      }
      subscriber.dataSources = dbs.toArray(new Array[DataSourceBase](0))

      subscribers.add(subscriber)
    }

    val host = InetAddress.getLocalHost
    val hostAddress = host.getHostAddress
    println("HOST: " + host.toString)
    println("HOSTADDRESS: " + hostAddress)

    //create and run all subscribers

    var pubAddress: InetAddress = InetAddress.getByName(publishers.get(0).ip)
    var subAddress: InetAddress = InetAddress.getByName(subscribers.get(0).ip)

    var akkaConfFile = new File(akkaConfigFilePath)

    if (subAddress.equals(host)) {
      println("Found Subscriber. Creating it.")
      var mainSub = new MainPubSub
      mainSub.loadConfiguration(akkaConfFile)
      mainSub.createActorSystem()
      mainSub.createSubscriber(subscribers.get(0).actorName)
      mainSub.attachShutdownHook()
    } else if (pubAddress.equals(host)) {
      //create and run all publishers
      val writer = new Writer
      var dataSourceProperties = publishers.get(0).dataSources(0).asInstanceOf[DataSource].properties.asInstanceOf[RDBMSDataSourceProperties]
      val driver = dataSourceProperties.driver
      val url = dataSourceProperties.url
      val username = dataSourceProperties.userName
      val password = dataSourceProperties.password
      val query = dataSourceProperties.query

      var (rs, cols) = writer.readDatabase(driver, url, username, password, query)
      writer.write(rs ,cols)
      var schema = writer.getSchemaJson()
      var os = writer.getByteArrayOutputStream()

      var mainPub = new MainPubSub
      mainPub.loadConfiguration(akkaConfFile)
      mainPub.createActorSystem()
      mainPub.createPubliser(publishers.get(0).actorName)
      Thread.sleep(5000)
      mainPub.publishSchema(schema)
      Thread.sleep(5000)
      mainPub.publishData(os.toByteArray)
      mainPub.attachShutdownHook()
    }
  }
}