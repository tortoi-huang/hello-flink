package org.huang.flink.entrypoint;

import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Collections;

public class CliFrontendApp {
    private static final Logger LOG = LoggerFactory.getLogger(CliFrontendApp.class);

    public static void main(String[] args) {
        fn1();
    }

    static void fn1() {
        try {
            final String configDir = "entrypoint/session/src/main/resources/conf";
            final String jar = "entrypoint/appdemo/build/libs/appdemo-1.0.0-SNAPSHOT.jar";
            URI jarUri = Paths.get(jar).toUri();
            //final Configuration configuration = GlobalConfiguration.loadConfiguration(configDir);
            final Configuration configuration = GlobalConfiguration.loadConfiguration(configDir,null);

            //本地执行
            configuration.set(DeploymentOptions.TARGET, "remote");
            // TODO 不太明白这个参数
            configuration.set(DeploymentOptions.ATTACHED, true);
            configuration.set(DeploymentOptions.SHUTDOWN_IF_ATTACHED, false);
            configuration.set(SavepointConfigOptions.RESTORE_MODE, RestoreMode.NO_CLAIM);
            configuration.set(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, false);
            configuration.set(PipelineOptions.JARS, Collections.singletonList(jarUri.toString()));
            //configuration.set(PipelineOptions.CLASSPATHS, false);


            PackagedProgram program = PackagedProgram.newBuilder()
                    .setJarFile(new File(jar))
                    //.setUserClassPaths(classpaths)
                    //.setEntryPointClassName(entryPointClass)
                    .setConfiguration(configuration)
                    //.setSavepointRestoreSettings(runOptions.getSavepointRestoreSettings())
                    //.setArguments("run")
                    .build();

            ClientUtils.executeProgram(new DefaultExecutorServiceLoader(), configuration, program, false, false);
            //program.invokeInteractiveModeForExecution();
        } catch (Exception e) {
            LOG.error("main: ", e);
        }
    }

    static void fn2() {
        CliFrontend.main(new String[]{"run", "entrypoint/appdemo/build/libs/appdemo-1.0.0-SNAPSHOT.jar","--configDir", "entrypoint/session/src/main/resources/conf"});
    }
}
