package de.tuberlin.dima.bdapro.muses.starter

class Publisher {
  var ip: String = null
  var port: Int = 0
  var actorName:String = "publisher"
  var subscriberPath: Array[String] = Array("/usr/subscriber")
  var dataSources: Array[DataSourceBase] = null

  override def toString = s"Publisher(ip=$ip, port=$port, actorName=$actorName, subscriberPath=$subscriberPath, dataSources=$dataSources)"
}