package de.tuberlin.dima.bdapro.muses.yarn.node;

//TODO: REFACTOR CODE, ENHANCE FUNCTIONALITY

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
public class ApplicationMaster {

    private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

    private static Options getCliArgs() {
        Options options = new Options();
        options.addOption("number_of_containers", true, "Number of containers for the Muses application.");
        options.addOption("priority", true, "Priority of the appplication. Default is .");
        options.addOption("container_memory", true, "Memory (in MBs) of the container in which Muses application will be running.");
        options.addOption("container_virtual_cores", true, "Virtual containers of the container in which Muses application will be running.");
        options.addOption("application_attempt_id", true, "Application ID for the attempt.");
        options.addOption("help", false, "Print usage");
        return options;
    }

    private static void printUsage(Options opts) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Client", opts);
    }

    public static void main(String[] args) throws Exception {
        ApplicationAttemptId applicationAttemptId = null;
        String appJarPath = "";
        String appConfFilePath = "";
        long appJarTimestamp = 0;
        long appConfFileTimestamp = 0;
        long appJarPathLen = 0;
        long appConfFilePathLen = 0;
        Configuration conf = new YarnConfiguration();
        Options options = ApplicationMaster.getCliArgs();
        CommandLine cliParser = null;
        try {
            cliParser = new GnuParser().parse(options, args);
        } catch (ParseException ex) {
            LOG.fatal("Exception has occured. ", ex);
        }
        if (cliParser.hasOption("help")) {
            ApplicationMaster.printUsage(ApplicationMaster.getCliArgs());
            return;
        }
        Map<String, String> envs = System.getenv();
        if (!envs.containsKey(ApplicationConstants.Environment.CONTAINER_ID.name())) {
            if (cliParser.hasOption("application_attempt_id")) {
                String appIdStr = cliParser.getOptionValue("application_attempt_id", "");
                applicationAttemptId = ConverterUtils.toApplicationAttemptId(appIdStr);
            } else {
                throw new IllegalArgumentException("Attempt ID not set in the environment. Please set Application attempt ID.");
            }
        } else {
            ContainerId containerId = ConverterUtils.toContainerId(envs.get(ApplicationConstants.Environment.CONTAINER_ID.name()));
            applicationAttemptId = containerId.getApplicationAttemptId();
        }

        if (!envs.containsKey(ApplicationConstants.Environment.NM_HTTP_PORT.name())) {
            throw new RuntimeException("The following is not set in env: " + ApplicationConstants.Environment.NM_HTTP_PORT);
        }
        if (!envs.containsKey(ApplicationConstants.Environment.NM_PORT.name())) {
            throw new RuntimeException("The following is not set in env: " + ApplicationConstants.Environment.NM_PORT.name());
        }
        if (!envs.containsKey(ApplicationConstants.Environment.NM_HOST.name())) {
            throw new RuntimeException("The following is not set in env: " + ApplicationConstants.Environment.NM_HOST.name());
        }
        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
            throw new RuntimeException("The following is not set in env: " + ApplicationConstants.APP_SUBMIT_TIME_ENV);
        }

        if (envs.containsKey("AM_JAR_PATH")) {
            appJarPath = envs.get("AM_JAR_PATH");
            LOG.debug("Jar path: " + appJarPath);

            if (envs.containsKey("AM_JAR_TIMESTAMP")) {
                appJarTimestamp = Long.valueOf(envs.get("AM_JAR_TIMESTAMP"));
            }

            if (envs.containsKey("AM_JAR_LENGTH")) {
                appJarPathLen = Long.valueOf(envs.get("AM_JAR_LENGTH"));
            }

            if (!appJarPath.isEmpty() && (appJarTimestamp <= 0 || appJarPathLen <= 0)) {
                LOG.error("Invalid values for the jar related environment variables. Values are: Jar Path: " + appJarPath  + ", Length: " + appJarPathLen + ", Timestamp: " + appJarTimestamp);
                throw new IllegalArgumentException("Invalid values for the jar related environment variables.");
            }
        }
        if (envs.containsKey("AM_CONF_FILE_PATH")) {
            appConfFilePath = envs.get("AM_CONF_FILE_PATH");

            if (envs.containsKey("AM_CONF_FILE_TIMESTAMP")) {
                appConfFileTimestamp = Long.valueOf(envs.get("AM_CONF_FILE_TIMESTAMP"));
            }
            if (envs.containsKey("AM_JAR_LENGTH")) {
                appConfFilePathLen = Long.valueOf(envs.get("AM_CONF_FILE_LENGTH"));
            }
            if (!appConfFilePath.isEmpty() && (appConfFileTimestamp <= 0 || appConfFilePathLen <= 0)) {
                LOG.error("Invalid values for the jar related environment variables. Values are: Jar Path: " + appConfFilePath  + ", Length: " + appConfFilePathLen + ", Timestamp: " + appConfFileTimestamp);
                throw new IllegalArgumentException("Invalid values for the config related environment variables.");
            }
        }
        int containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "512"));
        int containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_virtual_cores", "1"));
        int numTotalContainers = Integer.parseInt(cliParser.getOptionValue("number_of_containers", "1"));
        if (numTotalContainers == 0) {
            throw new IllegalArgumentException("Number of container is 0. Cannot run an application with 0 containers.");
        }
        int requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
        LOG.info("Starting application master");
        AMRMClient<ContainerRequest> amRMClient = AMRMClient.createAMRMClient();
        amRMClient.init(conf);
        amRMClient.start();
        LOG.info("Started application master resource manager client.");
        amRMClient.registerApplicationMaster("", 0, "");
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(containerMemory);
        capability.setVirtualCores(containerVirtualCores);
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(requestPriority);
        for (int i = 0; i < numTotalContainers; ++i) {
            ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
            amRMClient.addContainerRequest(containerAsk);
        }
        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();
        Map<String, String> containerEnv = new HashMap<String, String>();
        containerEnv.put("CLASSPATH", "./*");
        LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        if (!appJarPath.isEmpty()) {
            appMasterJar.setType(LocalResourceType.FILE);
            Path jarPath = new Path(appJarPath);
            jarPath = FileSystem.get(conf).makeQualified(jarPath);
            appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
            appMasterJar.setTimestamp(appJarTimestamp);
            appMasterJar.setSize(appJarPathLen);
            appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
        }

        LocalResource appConfFile = Records.newRecord(LocalResource.class);
        if (!appConfFilePath.isEmpty()) {
            appConfFile.setType(LocalResourceType.FILE);
            Path confFilePath = new Path(appConfFilePath);
            confFilePath = FileSystem.get(conf).makeQualified(confFilePath);
            appConfFile.setResource(ConverterUtils.getYarnUrlFromPath(confFilePath));
            appConfFile.setTimestamp(appJarTimestamp);
            appConfFile.setSize(appJarPathLen);
            appConfFile.setVisibility(LocalResourceVisibility.PUBLIC);
        }
        int allocatedContainers = 0;
        int completedContainers = 0;
        while (allocatedContainers < numTotalContainers) {
            AllocateResponse response = amRMClient.allocate(0);
            for (Container container : response.getAllocatedContainers()) {
                allocatedContainers++;
                ContainerLaunchContext appContainer = Records.newRecord(ContainerLaunchContext.class);
                Map<String, LocalResource> map = new HashMap<String, LocalResource>();
                map.put("AppMaster.jar", appMasterJar);
                map.put("muses-conf.json", appConfFile);
                appContainer.setLocalResources(map);
                appContainer.setEnvironment(containerEnv);
                appContainer.setCommands(Collections.singletonList("$JAVA_HOME/bin/java" + " -Xmx256M" + " de.tuberlin.dima.bdapro.muses.yarn.node.MusesStarter " + appConfFilePath + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));
                LOG.info("Launching the container");
                nmClient.startContainer(container, appContainer);
            }
            for (ContainerStatus s : response.getCompletedContainersStatuses()) {
                ++completedContainers;
                LOG.info("ContainerID:" + s.getContainerId() + ", state name:" + s.getState().name());
            }
            Thread.sleep(300);
        }
        while (completedContainers < numTotalContainers) {
            AllocateResponse response = amRMClient.allocate(completedContainers / numTotalContainers);
            for (ContainerStatus s : response.getCompletedContainersStatuses()) {
                ++completedContainers;
                LOG.info("Completed container information: ContainerID:" + s.getContainerId() + ", state name:" + s.getState().name());
            }
            Thread.sleep(300);
        }
        LOG.info("Completed:" + completedContainers);
        amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
        LOG.info("Finished execution of the application master.");
    }
}