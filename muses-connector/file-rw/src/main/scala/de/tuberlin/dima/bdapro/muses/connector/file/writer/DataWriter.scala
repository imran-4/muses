package de.tuberlin.dima.bdapro.muses.connector.file.writer

//write from arrow to file at target location (consumer | target | where data is migrated)
class DataWriter {
  def writeData(str: String): Unit = {
    println("FILE DATA WRITER>>>>>>>>>>>>> {}", str)
  }
}