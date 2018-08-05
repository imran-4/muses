package de.tuberlin.dima.bdapro.muses.connector.rdbms.reader

import java.sql._
import java.util

//TODO: REFACTOR IT LATER

class DataReader {
//  var connection: Connection = null
//  var prepareedStatement: PreparedStatement = null
//  var resultSet: ResultSet = null


  def loadDrivers(driver: String): Unit = Class.forName(driver)

  def getConnection(url: String, userName: String, password: String): Connection = DriverManager.getConnection(url, userName, password)

  @throws[SQLException]
  def getPreparedStatement(connection: Connection, sql: String): PreparedStatement = connection.prepareStatement(sql)

  @throws[SQLException]
  def getResultSet(statement: PreparedStatement): ResultSet = statement.executeQuery

  @throws[SQLException]
  def getResultSet(statement: CallableStatement): ResultSet = statement.executeQuery

  @throws[SQLException]
  def getMetaData(resultSet: ResultSet): ResultSetMetaData = resultSet.getMetaData

  @throws[SQLException]
  def getColumnCount(metaData: ResultSetMetaData) = metaData.getColumnCount

  @throws[SQLException]
  def getColumnNames(metaData: ResultSetMetaData): util.ArrayList[Tuple2[String, String]] = {
    var columns: util.ArrayList[Tuple2[String, String]] = new util.ArrayList[Tuple2[String, String]]
    for (i <- 1 to metaData.getColumnCount) {
      columns.add((metaData.getColumnName(i), metaData.getColumnTypeName(i)))
      }
    return columns
  }

  @throws[SQLException]
  def closeConnection(connection: Connection): Unit = connection.close()
}