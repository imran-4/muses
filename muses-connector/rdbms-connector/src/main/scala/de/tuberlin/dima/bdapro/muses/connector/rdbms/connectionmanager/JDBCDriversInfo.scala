package de.tuberlin.dima.bdapro.muses.connector.rdbms.connectionmanager

import java.util

import scala.collection.immutable.HashMap


//maybe later read it from properties file
object JDBCDriversInfo {
  var MYSQL_DRIVER: String = "com.mysql.cj.jdbc.Driver"
  var POSTGRES_DRIVER: String = "org.postgresql.Driver"
  var IBMDB2_DRIVER: String = "com.ibm.db2.jdbc.app.DB2Driver"
  var MSSQL_DRIVER: String = "com.microsoft.sqlserver.jdbc.SQLServerDrive"
  var MSSQL_WEBLOGIC_DRIVER = "weblogic.jdbc.mssqlserver4.Driver"
  var MSSQL_SPRINTA_DRIVER = "com.inet.tds.TdsDriver"
  var MSSQL_JTURBO_DRIVER = "com.ashna.jturbo.driver.Driver"
  var ORACLE_THIN_DRIVER: String = "oracle.jdbc.driver.OracleDriver"
  var ORACLE_ORNAHO_DRIVER: String = "com.inet.pool.PoolDriver"
  var SYBASE_DRIVER: String = "ncom.sybase.jdbc2.jdbc.SybDriver"
  var POINTBASEEMBEDDEDSERVER_DRIVER: String = "RmiJdbc.RJDriver"
  var CLOUDSCAPE_DRIVER: String = "COM.cloudscape.core.JDBCDriver"
  var CLOUDSCAPERMI_DRIVER: String = "com.mysql.jdbc.Driver"
  var FIREBIRD_DRIVER: String = "org.firebirdsql.jdbc.FBDriver"
  var IDSSERVER_DRIVER: String = "ids.sql.IDSDriver"
  var INFORMIXDYNAMICSERVER_DRIVER: String = "com.informix.jdbc.IfxDriver"
  var INSTANTDB_DRIVER: String = "org.enhydra.instantdb.jdbc.idbDriver"
  var INTERBASE_DRIVER: String = "interbase.interclient.Driver"
  var HYPERSONICSQL_DRIVER: String = "org.hsql.jdbcDriver"
}
/*
MySql/
com.mysql.jdbc.Driver 	jdbc:mysql://:/?user=&password= 	download
PostgreSQL/
org.postgresql.Driver 	dbc:postgresql://:/?user=&password= 	distributed with soapUI Pro
IBMDB2/
COM.ibm.db2.jdbc.app.DB2Driver 	jdbc:db2://:/?user=&password= 	comes with db2 express c download
MSSQL(MicrosoftDriver)/
com.microsoft.sqlserver.jdbc.SQLServerDrive 	jdbc:microsoft:sqlserver://:;databaseName=?user=&password= 	distributed with soapUI Pro
MSSQL(Weblogic)/
weblogic.jdbc.mssqlserver4.Driver 	jdbc:weblogic:mssqlserver4:@: 	should come with weblogic instalation
MSSQL(SprintaDriver)/
com.inet.tds.TdsDriver 	jdbc:inetdae::?database=?user=&password= 	(*)download
MicrosoftSQLServer(JTurboDriver)/
com.ashna.jturbo.driver.Driver 	jdbc:JTurbo://://user=/password= 	download
OracleThin/
oracle.jdbc.driver.OracleDriver 	jdbc:oracle:thin:/@:: 	download
Oracle(OranhoDriver)/
com.inet.pool.PoolDriver 	jdbc:inetpool:inetora:?database=&user=&password=&sid= 	(*)download
Sybase(jConnect5.2)/
ncom.sybase.jdbc2.jdbc.SybDriver 	jdbc:sybase:Tds::?user=&password= 	download
PointBaseEmbeddedServer/
com.pointbase.jdbc.jdbcUniversalDriver 	jdbc:pointbase://embedded:/?user=&password= 	Get more information here
Cloudscape/
COM.cloudscape.core.JDBCDriver 	jdbc:cloudscape:?user=&password= 	The Cloudscape Embedded JDBC Driver is included with the Cloudscape product. You can download Cloudscape and this driver from this page
CloudscapeRMI/
RmiJdbc.RJDriver 	jdbc:rmi://:/jdbc:cloudscape:?user=&password= 	Get more information here
Firebird(JCA-JDBCDriver)/
org.firebirdsql.jdbc.FBDriver 	jdbc:firebirdsql://:/?user=&password= 	The Firebird JCA-JDBC Driver is bundled in a jaybird JAR file which is available as part of the Firebird download
IDSServer/
ids.sql.IDSDriver 	jdbc:ids://:/conn?dsn='' 	Get more information here
InformixDynamicServer/
com.informix.jdbc.IfxDriver 	jdbc:informix-sqli://:/:INFORMIXSERVER= 	download
InstantDB/
org.enhydra.instantdb.jdbc.idbDriver 	jdbc:idb: 	Get more information here
Interbase(InterClientDriver)/
interbase.interclient.Driver 	jdbc:interbase:/// 	The InterBase JDBC driver is bundled in a JAR file called 'interclient.jar' which is available as part of the Interclient download.
HypersonicSQL/
org.hsql.jdbcDriver 	jdbc:HypersonicSQL:?user=&password= 	download*/