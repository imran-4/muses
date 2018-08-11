package de.tuberlin.dima.bdapro.muses.starter

class RDBMSDataSourceProperties extends DataSourceProperties {
  var driver:String = null
  var url: String = null
  var userName: String = null
  var password: String = null
  var query: String = null
  var numberOfPartitions: Int = 1
  var partitionKey: String = null

  override def toString = s"RDBMSDataSourceProperties(driver=$driver, url=$url, userName=$userName, password=$password, query=$query, numberOfPartitions=$numberOfPartitions, partitionKey=$partitionKey)"
}