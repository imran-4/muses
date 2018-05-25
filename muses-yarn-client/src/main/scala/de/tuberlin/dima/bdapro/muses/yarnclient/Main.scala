package de.tuberlin.dima.bdapro.muses.yarnclient

import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.conf.YarnConfiguration
import com.sun.tools.javac.util.Assert
import org.apache.hadoop.yarn.conf.YarnConfiguration
import java.net.InetAddress
import java.util

import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse
import org.apache.hadoop.yarn.client.api.YarnClientApplication

object Main  {

  def main(args: Array[String]): Unit = {
    var conf = getConfiguration
    val yarnClient = YarnClient.createYarnClient
    yarnClient.init(conf)
    yarnClient.start()


    val app = yarnClient.createApplication
    val appResponse = app.getNewApplicationResponse


    //................................

    // set the application submission context
    var appContext = app.getApplicationSubmissionContext
    var appId = appContext.getApplicationId()

    appContext.setKeepContainersAcrossApplicationAttempts(true);
    appContext.setApplicationName("test");
  }

  def getConfiguration(): Configuration = {

    val conf = new YarnConfiguration
    var localhostAddress = ""
    try
      localhostAddress = InetAddress.getByName("localhost").getCanonicalHostName
    catch {
      case e: Exception =>
        println("failed")
    }
    conf.setInt(YarnConfiguration.NM_PMEM_MB, 5 * 1024) // 5GB

    conf.set(YarnConfiguration.NM_ADDRESS, localhostAddress + ":12345")
    conf.set(YarnConfiguration.NM_LOCALIZER_ADDRESS, localhostAddress + ":12346")
//    conf.set(YarnConfiguration.NM_LOG_DIRS, logsDir.getAbsolutePath)
//    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, remoteLogsDir.getAbsolutePath)
//    conf.set(YarnConfiguration.NM_LOCAL_DIRS, nmLocalDir.getAbsolutePath)
    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 1)

    return conf
  }
}
