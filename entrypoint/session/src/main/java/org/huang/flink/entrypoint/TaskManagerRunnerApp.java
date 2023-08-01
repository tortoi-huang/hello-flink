package org.huang.flink.entrypoint;

import org.apache.flink.configuration.*;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.*;
import org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class TaskManagerRunnerApp {
    private static final Logger LOG = LoggerFactory.getLogger(TaskManagerRunnerApp.class);
    public static void main(String[] args) {
        try {
            Configuration configuration = getFileConfig();

            final PluginManager pluginManager =
                    PluginUtils.createPluginManagerFromRootFolder(configuration);
            TaskManagerRunner taskManagerRunner =
                    new TaskManagerRunner(configuration, pluginManager, TaskManagerRunner::createTaskExecutorService);
            taskManagerRunner.start();
        } catch (Exception e) {
            LOG.error("main: ", e);
        }
    }



    private static Configuration getFileConfig() throws FlinkParseException, IOException {
        String[] args = {"--configDir", "entrypoint/session/src/main/resources/conf"};
        Configuration configuration = ConfigurationParserUtils.loadCommonConfiguration(
                args, TaskManagerRunner.class.getSimpleName());
        TaskExecutorResourceUtils.adjustForLocalExecution(configuration);

        File tmpDirectory = Files.createTempDirectory("flink_tmp").toFile();
        configuration.set(ClusterOptions.PROCESS_WORKING_DIR_BASE, tmpDirectory.getAbsolutePath());

        ResourceID foobar = new ResourceID("task1");
        configuration.set(TaskManagerOptions.TASK_MANAGER_RESOURCE_ID, foobar.toString());
        return configuration;
    }
}
