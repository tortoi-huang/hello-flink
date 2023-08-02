package org.huang.flink.entrypoint;

import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Method;
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
            final String jar = "entrypoint/appdemo/build/libs/appdemo-1.0.0-SNAPSHOT.jar";
            URI jarUri = Paths.get(jar).toUri();

            // 本地执行
//            final Configuration configuration = new Configuration();
//            configuration.set(DeploymentOptions.TARGET, "local");
            // 远程执行
            final String configDir = "entrypoint/session/src/main/resources/conf";
            final Configuration configuration = GlobalConfiguration.loadConfiguration(configDir);
            configuration.set(DeploymentOptions.TARGET, "remote");
            // TODO 不太明白这个参数
            configuration.set(DeploymentOptions.ATTACHED, true);
            configuration.set(DeploymentOptions.SHUTDOWN_IF_ATTACHED, false);
            configuration.set(SavepointConfigOptions.RESTORE_MODE, RestoreMode.NO_CLAIM);
            configuration.set(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, false);
            // 这个配置要提交到服务器的jar，没有配置会导致class not found exception
            configuration.set(PipelineOptions.JARS, Collections.singletonList(jarUri.toString()));
            //configuration.set(PipelineOptions.CLASSPATHS, false);

            //runInCurrentThread(configuration, jar);
            runInNewThread(configuration, jarUri);
            //program.invokeInteractiveModeForExecution();
        } catch (Exception e) {
            LOG.error("main: ", e);
        }
    }

    private static void runInCurrentThread(Configuration configuration, final String jar) throws ProgramInvocationException {
        PackagedProgram program = PackagedProgram.newBuilder()
                .setJarFile(new File(jar))
                //.setUserClassPaths(classpaths)
                //.setEntryPointClassName(entryPointClass)
                .setConfiguration(configuration)
                //.setSavepointRestoreSettings(runOptions.getSavepointRestoreSettings())
                //.setArguments("run")
                .build();
        ClientUtils.executeProgram(new DefaultExecutorServiceLoader(), configuration, program, false, false);
    }

    private static void runInNewThread(Configuration configuration, URI jarUri) throws InterruptedException {
        Thread thread = new Thread(() -> {
            try {
                //如果是线程池，或者当前线程在执行job完成后还需要处理其他事情，则要执行结束后恢复classloader
                final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
                PipelineExecutorServiceLoader executorServiceLoader = new DefaultExecutorServiceLoader();
                //用户编写的flink代码在这个loader中加载
                final ClassLoader userCodeClassLoader = ClientUtils.buildUserCodeClassLoader(Collections.singletonList(jarUri.toURL()), Collections.emptyList(),
                        contextClassLoader, configuration);
                Thread.currentThread().setContextClassLoader(userCodeClassLoader);

                // 配置StreamExecutionEnvironment.getExecutionEnvironment();的运行环境的上下文参数,
                // 如果是批处理 ExecutionEnvironment.getExecutionEnvironment() 则设置 ContextEnvironment.setAsContext
                StreamContextEnvironment.setAsContext(
                        executorServiceLoader,
                        configuration,
                        userCodeClassLoader,
                        false,
                        false);

                //加载运行的main类，实际应该动态根据jar的main-class配置加载，这里写死测试
                Class<?> mainClass = userCodeClassLoader.loadClass("org.huang.flink.entrypoint.appdemo.App");
                Method mainMethod = mainClass.getDeclaredMethod("main", String[].class);
                mainMethod.invoke(null, (Object) null);
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thread.start();
        thread.join();
    }
}
