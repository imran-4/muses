package de.tuberlin.dima.bdapro.muses.yarnclient;

// TODO: Refactor code, add more command line options

import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import java.io.IOException;
import java.util.*;

//COMMAND: $HADOOP_HOME/bin/yarn jar muses-yarn-client/target/muses-yarn-client-1.0.0-SNAPSHOT.jar de.tuberlin.dima.bdapro.muses.yarnclient.Client -jar muses-yarn-appmaster/target/muses-yarn-appmaster-1.0.0-SNAPSHOT.jar -number_of_containers 1 -app_jar muses-starter/target/muses-starter-1.0.0-SNAPSHOT.jar -conf muses-config.json
public class Client {
    private static final Log LOG = LogFactory.getLog(Client.class);

    // functions for command line arguments
    private static void printUsage(Options opts) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Client", opts);
    }
    private static Options getCliArgs() {
        Options options = new Options();
        options.addOption("appname", true, "Name of the application. Default value: Muses");
        options.addOption("number_of_containers", true, "Number of containers for the Muses application.");
        options.addOption("priority", true, "Priority of the application. Default: 0");

        options.addOption("application_master_vcores", true, "Virtaul cores for the application master.");
        options.addOption("application_master_memory", true, "Memory (in MBs) for the application master.");

        options.addOption("jar", true, "JAR file containing the application master.");
        options.addOption("app_jar", true, "JAR file containing the application.");
        options.addOption("conf", true, "Configuration file for the nodes running Muses.");
        options.addOption("akka_conf", true, "Configuration file for the nodes running Muses.");

        options.addOption("queue", true, "Resource Manager Queue.");

        options.addOption("container_memory", true, "Memory (in MBs) of the container in which Muses application will be running.");
        options.addOption("container_virtual_cores", true, "Virtual containers of the container in which Muses application will be running.");

        options.addOption("help", false, "Print Usage.");
        return options;
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            LOG.fatal("No arguments are provided to the program.");
            throw new IllegalArgumentException("No arguments are provided to the client.");
        }
        //get all options
        Options options = Client.getCliArgs();
        // creating yarn client and configuration
        LOG.info("Creating YARN Client.");
        YarnClient client = YarnClient.createYarnClient();
        Configuration conf = new YarnConfiguration();
        client.init(conf);
        long startTime = System.currentTimeMillis();

        CommandLine cliParser = null;
        try {
            cliParser = new GnuParser().parse(options, args);
        } catch (ParseException ex) {
            LOG.fatal("Exception has occured. ", ex);
        }

        if (cliParser.hasOption("help")) {
            Client.printUsage(Client.getCliArgs());
            return;
        }

        String applicationName = cliParser.getOptionValue("appname", "Muses");
        int applicationMasterPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
        String applicationMasterQueue = cliParser.getOptionValue("queue", "default");
        int applicationMasterVCores = Integer.parseInt(cliParser.getOptionValue("master_vcores", "2"));
        int applicationMasterMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "1024"));

        if (applicationMasterMemory < 0) {
            throw new IllegalArgumentException("The memory size specified is not valid. Please specify a valid size memory.");
        }
        if (applicationMasterVCores < 0) {
            throw new IllegalArgumentException("The virtual cores specified is not valid. Please specify valid virtual cores.");
        }

        if (!cliParser.hasOption("jar")) {
            throw new IllegalArgumentException("No jar file specified for application master. Please specify a jar file to run application master.");
        }

        String applicationMasterJarPath = cliParser.getOptionValue("jar");
        String applicationJarPath = cliParser.getOptionValue("app_jar");
        String applicationConfFilePath = cliParser.getOptionValue("conf");
        String akkaConfFilePath = cliParser.getOptionValue("akka_conf");
        int containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "512"));
        int containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
        int numContainers = Integer.parseInt(cliParser.getOptionValue("number_of_containers", "1"));
        if (containerMemory < 0) {
            throw new IllegalArgumentException("The specified container memory is not valid.");
        }
        if (containerVirtualCores < 0) {
            throw new IllegalArgumentException("The specified container virtual cores is not valid.");
        }
        if (numContainers <=0 ) {
            throw new IllegalArgumentException("Number of containers should be >= 1.");
        }
        int clientTimeout = Integer.parseInt(cliParser.getOptionValue("timeout", "600000"));

        LOG.info("Starting Apache YARN client.");
        client.start();

        LOG.info("Apache YARN client is started.");

        YarnClientApplication app = null;
        try {
            app = client.createApplication();
        } catch (YarnException e) {
            LOG.fatal("YARN Exception has occured. ", e);
            e.printStackTrace();
        } catch (IOException e) {
            LOG.fatal("IOException has occured. ", e);
            e.printStackTrace();
        }
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        int maximumMemory = appResponse.getMaximumResourceCapability().getMemory();
        if (applicationMasterMemory > maximumMemory) {
            applicationMasterMemory = maximumMemory;
        }
        int maximumVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
        if (applicationMasterVCores > maximumVCores) {
            applicationMasterVCores = maximumVCores;
        }
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();

        appContext.setApplicationName(applicationName);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(applicationMasterMemory);
        capability.setVirtualCores(applicationMasterVCores);
        appContext.setResource(capability);

        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(applicationMasterPriority);
        appContext.setPriority(pri);

        appContext.setQueue(applicationMasterQueue);

        ContainerLaunchContext applicationMasterContainer = Records.newRecord(ContainerLaunchContext.class);
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            LOG.fatal("IOException has occured.");
            e.printStackTrace();
        }
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        LOG.info("Copying application master jar file from local system and adding to local environment.");

        try {
            Client.addResources(applicationName, fs, applicationMasterJarPath, "AppMaster.jar", appId.getId(),
                    localResources, null);
            Client.addResources(applicationName, fs, applicationJarPath, "App.jar", appId.getId(),
                    localResources, null);
            Client.addResources(applicationName, fs, applicationConfFilePath, "muses-conf.json", appId.getId(),
                    localResources, null);
            Client.addResources(applicationName, fs, akkaConfFilePath, "application.conf", appId.getId(),
                    localResources, null);
        } catch (IOException ex) {
            LOG.fatal("IOException has occured. ", ex);
        }

        applicationMasterContainer.setLocalResources(localResources);
        LOG.info("Setting the environment for the application master.");

        //setting up environments
        try {
            applicationMasterContainer.setEnvironment(Client.getAMEnvironment(localResources, fs, conf));
        } catch (Exception ex) {
            LOG.fatal("IOException has occured. ", ex);
        }

        Vector<CharSequence> vargs = new Vector<CharSequence>(30);

        LOG.info("Setting up app master command.");
        vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
        vargs.add("-Xmx" + applicationMasterMemory + "m");
        vargs.add("de.tuberlin.dima.bdapro.muses.yarn.node.ApplicationMaster");
        vargs.add("--container_memory " + String.valueOf(containerMemory));
        vargs.add("--container_virtual_cores " + String.valueOf(containerVirtualCores));
        vargs.add("--number_of_containers " + String.valueOf(numContainers));
        vargs.add("--priority " + String.valueOf(0));
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        LOG.info("Completed setting up app master command " + command.toString());
        List<String> commands = new ArrayList<String>();
        commands.add(command.toString());
        applicationMasterContainer.setCommands(commands);

        appContext.setAMContainerSpec(applicationMasterContainer);

        //>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        LOG.info("Submitting application to application master.");

        try {
            client.submitApplication(appContext);
        } catch (YarnException e) {
            LOG.fatal("YARN Exception has occured. ", e);
            e.printStackTrace();
        } catch (IOException e) {
            LOG.fatal("IOException has occured. ", e);
            e.printStackTrace();
        }

        boolean status = true;
        //wait
        while (true) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                LOG.error("Exception has occured. ", e);
            }

            ApplicationReport report = null;
            try {
                report = client.getApplicationReport(appId);
            } catch (YarnException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    LOG.info("Application has completed successfully. "
                            + " Breaking monitoring loop : ApplicationId:" + appId.getId());
                    status= true; break;
                }
                else {
                    LOG.info("Application did finished unsuccessfully."
                            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                            + ". Breaking monitoring loop : ApplicationId:" + appId.getId());
                    status= false; break;
                }
            }
            else if (YarnApplicationState.KILLED == state || YarnApplicationState.FAILED == state) {
                LOG.info("Application did not finish."
                        + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                        + ". Breaking monitoring loop : ApplicationId:" + appId.getId());
                status= false; break;
            } else if (System.currentTimeMillis() > (startTime + clientTimeout)) {
                LOG.info("Reached client specified timeout for application. Killing application"
                        + ". Breaking monitoring loop : ApplicationId:" + appId.getId());
                try {
                    client.killApplication(appId);
                } catch (YarnException ex) {
                    LOG.fatal("YARN Exception has occured. ", ex);
                    ex.printStackTrace();
                } catch (IOException ex) {
                    LOG.fatal("IOException has occured. ", ex);
                    ex.printStackTrace();
                }
                status= false; break;
            }
        }

        if (status) {
            LOG.info("Application completed successfully.");
            System.exit(0);
        }
        LOG.error("Application failed to complete successfully");
        System.exit(2);
    }

    private static Map<String, String> getAMEnvironment(Map<String, LocalResource> localResources
            , FileSystem fs, Configuration conf) throws IOException{
        Map<String, String> env = new HashMap<String, String>();

        //...............................
        LocalResource appMasterJarResource = localResources.get("AppMaster.jar");

        Path hdfsAppMasterJarPath = new Path(fs.getHomeDirectory(), appMasterJarResource.getResource().getFile());
        FileStatus hdfsAppMasterJarStatus = fs.getFileStatus(hdfsAppMasterJarPath);
        long hdfsAppMasterJarLength = hdfsAppMasterJarStatus.getLen();
        long hdfsAppMasterJarTimestamp = hdfsAppMasterJarStatus.getModificationTime();

        env.put("AM_JAR_PATH", hdfsAppMasterJarPath.toString());
        env.put("AM_JAR_TIMESTAMP", Long.toString(hdfsAppMasterJarTimestamp));
        env.put("AM_JAR_LENGTH", Long.toString(hdfsAppMasterJarLength));

        //...............................
        LocalResource appJarResource = localResources.get("App.jar");

        Path hdfsAppJarPath = new Path(fs.getHomeDirectory(), appJarResource.getResource().getFile());
        FileStatus hdfsAppJarStatus = fs.getFileStatus(hdfsAppJarPath);
        long hdfsAppJarLength = hdfsAppJarStatus.getLen();
        long hdfsAppJarTimestamp = hdfsAppJarStatus.getModificationTime();

        LOG.info("Application JAR File Path: " + hdfsAppJarPath.toString());

        env.put("APP_JAR_PATH", hdfsAppJarPath.toString());
        env.put("APP_JAR_TIMESTAMP", Long.toString(hdfsAppJarTimestamp));
        env.put("APP_JAR_LENGTH", Long.toString(hdfsAppJarLength));

        //...............................
        LocalResource appConfFileResource = localResources.get("muses-conf.json");
        Path hdfsAppConfFilePath = new Path(fs.getHomeDirectory(), appConfFileResource.getResource().getFile());
        FileStatus hdfsAppConfFileStatus = fs.getFileStatus(hdfsAppConfFilePath);
        long hdfsAppConfFileLength = hdfsAppConfFileStatus.getLen();
        long hdfsAppConfFileTimestamp = hdfsAppConfFileStatus.getModificationTime();

        LOG.info("Conf File Path: " + hdfsAppConfFilePath.toString());

        env.put("AM_CONF_FILE_PATH", hdfsAppConfFilePath.toString());
        env.put("AM_CONF_FILE_TIMESTAMP", Long.toString(hdfsAppConfFileTimestamp));
        env.put("AM_CONF_FILE_LENGTH", Long.toString(hdfsAppConfFileLength));

        //...............................
        LocalResource akkaConfFileResource = localResources.get("application.conf");
        Path hdfsAkkaConfFilePath = new Path(fs.getHomeDirectory(), akkaConfFileResource.getResource().getFile());
        FileStatus hdfsAkkaConfFileStatus = fs.getFileStatus(hdfsAkkaConfFilePath);
        long hdfsAkkaConfFileLength = hdfsAkkaConfFileStatus.getLen();
        long hdfsAkkaConfFileTimestamp = hdfsAkkaConfFileStatus.getModificationTime();

        LOG.info("AKKA Conf File Path: " + hdfsAkkaConfFilePath.toString());

        env.put("AKKA_CONF_FILE_PATH", hdfsAkkaConfFilePath.toString());
        env.put("AKKA_CONF_FILE_TIMESTAMP", Long.toString(hdfsAkkaConfFileTimestamp));
        env.put("AKKA_CONF_FILE_LENGTH", Long.toString(hdfsAkkaConfFileLength));

        //...............................


        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        env.put("CLASSPATH", classPathEnv.toString());

        return env;
    }

    private static void addResources(String appName, FileSystem fs, String fileSrcPath,
                                     String fileDstPath, int appId, Map<String, LocalResource> localResources,
                                     String resources) throws IOException {
        String suffix = appName + "/" + appId + "/" + fileDstPath;
        Path dst =
                new Path(fs.getHomeDirectory(), suffix);
        if (fileSrcPath == null) {
            FSDataOutputStream ostream = null;
            try {
                ostream = FileSystem
                        .create(fs, dst, new FsPermission((short) 0710));
                ostream.writeUTF(resources);
            } finally {
                IOUtils.closeQuietly(ostream);
            }
        } else {
            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        }
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc =
                LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(fileDstPath, scRsrc);
    }

}
