package de.tuberlin.dima.bdapro.muses.connector.rdbms.writer

import java.sql._
import java.util

class DataWriter {
  def loadDrivers(driver: String): Unit = Class.forName(driver)

  def getConnection(url: String, userName: String, password: String): Connection = DriverManager.getConnection(url, userName, password)

  def getQuery(tableName: String, columnNameTypeTuples: util.HashMap[String, String]): String = {
    var insertQuery = new StringBuilder
    var commaSepartedColumns = new StringBuilder
    commaSepartedColumns.append(columnNameTypeTuples.keySet().toArray.mkString(","))
    insertQuery.append("INSERT INTO {} ({}) VALUES ({})", tableName, commaSepartedColumns.toString, "?," * commaSepartedColumns.length ) //TODO: check if we need to remove last comma
    return insertQuery.toString()
  }

  def setPreparedSetValues(columnType: String, preparedStatement: PreparedStatement, index: Int, value: Any): Unit = {
    var columnTypeString = columnType.toUpperCase
    columnType match {
      case "INT" | "INTEGER"  => preparedStatement.setInt(index, value.asInstanceOf[Int])
      case "TINYINT" | "BYTE" => preparedStatement.setByte(index, value.asInstanceOf[Byte])
      case "SMALLINT" | "SHORT" => preparedStatement.setShort(index, value.asInstanceOf[Short])
      case "BIGINT" | "LONG" => preparedStatement.setLong(index, value.asInstanceOf[Long])
      case "NUMERIC" | "DECIMAL" => preparedStatement.setBigDecimal(index, value.asInstanceOf[java.math.BigDecimal])
      case "FLOAT" | "REAL" => preparedStatement.setFloat(index, value.asInstanceOf[Float])
      case "DOUBLE" => preparedStatement.setDouble(index,value.asInstanceOf[Double])
      case "BINARY" => preparedStatement.setBoolean(index, value.asInstanceOf[Boolean])
      case "VARBINARY" => preparedStatement.setBoolean(index, value.asInstanceOf[Boolean])
      case "TIME" => preparedStatement.setTime(index, value.asInstanceOf[Time])
      case "TIMESTAMP" => preparedStatement.setTimestamp(index, value.asInstanceOf[Timestamp])
      case "DATE"  => preparedStatement.setDate(index, value.asInstanceOf[Date])
      case "VARCHAR" | "CHAR" | "LONGVARCHAR"  => preparedStatement.setString(index, value.asInstanceOf[String])
      case _  => throw new Exception("Type not supported so far. Please check in the coming releases.")
    }
  }

  @throws[SQLException]
  def getPreparedStatement(connection: Connection, tableName: String, columnNameTypeTuples: util.HashMap[String, String]): PreparedStatement = {
    val preparedStatement = connection.prepareStatement(getQuery(tableName, columnNameTypeTuples))
    var index = 1
    columnNameTypeTuples.keySet().forEach(x => {
      setPreparedSetValues(columnNameTypeTuples.get(x), preparedStatement, index, null)//get values as argument of this fucntion
      index += 1
    })
    return preparedStatement
  }

  @throws[SQLException]
  def executePrepartedStatement(statement: PreparedStatement): Boolean = statement.execute()

  @throws[SQLException]
  def closeConnection(connection: Connection): Unit = connection.close()
}