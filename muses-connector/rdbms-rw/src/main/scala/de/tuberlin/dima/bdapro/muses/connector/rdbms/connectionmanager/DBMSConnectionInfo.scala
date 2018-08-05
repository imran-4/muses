package de.tuberlin.dima.bdapro.muses.connector.rdbms.connectionmanager

class DBMSConnectionInfo () {
  private var _jdbcDriver: String = null //= //"com.mysql.jdbc.Driver"
  private var _jdbcConnectionString: String = null // = "jdbc:mysql://localhost/employees?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
  private var _userName:String = null
  private var _password: String = null

  def this(jdbcDriver: String, jdbcConnectionString: String, userName: String, password: String) {
    this()
    this._jdbcDriver = jdbcDriver
    this._jdbcConnectionString = jdbcConnectionString
    this._userName = userName
    this._password = password
  }

  def jdbcDriver = _jdbcDriver

  def jdbcDriver_= (jdbcDriver: String):Unit = this._jdbcDriver = jdbcDriver

  def jdbcConnectionString = _jdbcDriver

  def jdbcConnectionString_= (jdbcConnectionString: String):Unit = this._jdbcConnectionString = jdbcConnectionString

  def userName = _userName

  def userName_= (userName: String):Unit = this._userName = userName

  def password = _password

  def password_= (password: String):Unit = this._password = password
}