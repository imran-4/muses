package de.tuberlin.dima.bdapro.muses.starter

class Subscriber {
  var ip:String = null
  var port: Int = 0
  var actorName: String = "subscriber"
  var dataSources: Array[DataSourceBase] = null

  override def toString = s"Subscriber(ip=$ip, port=$port, actorName=$actorName, dataSources=$dataSources)"
}