package de.tuberlin.dima.bdapro.muses.starter

import java.nio.file.{Files, Paths}

import de.tuberlin.dima.bdapro.muses.akka.main.MainPubSub
import de.tuberlin.dima.bdapro.muses.connector.arrow.writer.Writer
import de.tuberlin.dima.bdapro.muses.connector.rdbms.connectionmanager.JDBCDriversInfo
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.io.IOException
import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.Files
import java.nio.file.Paths
import java.util

import com.google.gson.{Gson, JsonObject, JsonParser}

object MusesStarter {

//  def jsonStringToMap(jsonString: String): Map[String, Any] = {
//    implicit val formats = org.json4s.DefaultFormats
//
//    parse(jsonString).extract[Map[String, Any]]
//  }

  def jsonStringToMap(jsonString: String): JsonObject = {
    import com.google.gson.JsonObject
    val jsonObject = new JsonParser().parse(jsonString).getAsJsonObject
    return jsonObject
  }

  @throws[IOException]
  def readFile(path: String, encoding: Charset): String = {
    val encoded = Files.readAllBytes(Paths.get(path))
    new String(encoded, encoding)
  }

  def main(args: Array[String]): Unit = {

    val configFilePath = args(0).trim
    val content: String = readFile(configFilePath, StandardCharsets.UTF_8)
    var configurations = jsonStringToMap(content)

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

    var rol = "sub"
    if (rol == "pub") {
      val writer = new Writer
      val driver = JDBCDriversInfo.MYSQL_DRIVER
      val url = "jdbc:mysql://localhost/employees?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
      val username = "root"
      val password = "root"
      val query = "SELECT * FROM employees"

      var (rs, cols) = writer.readDatabase(driver, url, username, password, query)
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