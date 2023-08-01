package org.huang.flink.entrypoint;

import org.apache.flink.configuration.*;
import org.apache.flink.runtime.entrypoint.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;

public class SessionClusterEntrypointApp {
    private static final Logger LOG = LoggerFactory.getLogger(SessionClusterEntrypointApp.class);

    /**
     * 启动一个job manager
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        Configuration configuration = getFileConfig();

        try (final StandaloneSessionClusterEntrypoint clusterEntrypoint =
                     new StandaloneSessionClusterEntrypoint(configuration)) {
            //clusterEntrypoint.startCluster();
            ClusterEntrypoint.runClusterEntrypoint(clusterEntrypoint);
        } catch (Exception e) {
            LOG.error("main: ", e);
        }

    }

    private static Configuration getFileConfig() {
        String[] args = {"--configDir", "entrypoint/session/src/main/resources/conf", "--executionMode", "cluster"};
        final EntrypointClusterConfiguration entrypointClusterConfiguration =
                ClusterEntrypointUtils.parseParametersOrExit(
                        args,
                        new EntrypointClusterConfigurationParserFactory(),
                        StandaloneSessionClusterEntrypoint.class);
        //return ClusterEntrypoint.loadConfiguration(entrypointClusterConfiguration);
        final Configuration dynamicProperties =
                ConfigurationUtils.createConfiguration(
                        entrypointClusterConfiguration.getDynamicProperties());
        final Configuration configuration =
                GlobalConfiguration.loadConfiguration(
                        entrypointClusterConfiguration.getConfigDir(), dynamicProperties);

        final int restPort = entrypointClusterConfiguration.getRestPort();

        if (restPort >= 0) {
            LOG.warn(
                    "The 'webui-port' parameter of 'jobmanager.sh' has been deprecated. Please use '-D {}=<port> instead.",
                    RestOptions.PORT);
            configuration.setInteger(RestOptions.PORT, restPort);
        }

        final String hostname = entrypointClusterConfiguration.getHostname();

        if (hostname != null) {
            LOG.warn(
                    "The 'host' parameter of 'jobmanager.sh' has been deprecated. Please use '-D {}=<host> instead.",
                    JobManagerOptions.ADDRESS);
            configuration.setString(JobManagerOptions.ADDRESS, hostname);
        }
        return configuration;
    }

    private static Configuration getProgramConfig() throws IOException {
        //临时文件目录
        File tmpDirectory = Files.createTempDirectory("").toFile();

        final Configuration configuration = new Configuration();
        //configuration.set(JobManagerOptions.ADDRESS, "localhost");
        //configuration.set(JobManagerOptions.PORT, 9090);
        //configuration.set(RestOptions.BIND_PORT, "9091");
        configuration.set(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, 10000L);
        configuration.set(HeartbeatManagerOptions.HEARTBEAT_INTERVAL, 1000L);
        configuration.set(HeartbeatManagerOptions.HEARTBEAT_RPC_FAILURE_THRESHOLD, 1);
        configuration.set(ClusterOptions.PROCESS_WORKING_DIR_BASE, tmpDirectory.getAbsolutePath());
        configuration.set(CheckpointingOptions.LOCAL_RECOVERY, true);
        configuration.set(TaskManagerOptions.SLOT_TIMEOUT, Duration.ofSeconds(30L));
        return configuration;
    }
}

