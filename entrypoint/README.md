# 组件测试
这里测试通过编程的方式启动flink各个组件

1. org.huang.flink.entrypoint.SessionClusterEntrypointApp 启动一个jobmanager， 还包括dispacher和ResourceManager
2. org.huang.flink.entrypoint.TaskManagerRunnerApp 启动一个taskmanager
3. org.huang.flink.entrypoint.CliFrontendApp 解析一个flinkjar包并其提交到jobmanager

## flink 提交流程
1. 使用客户端提交 $FLINK_HOME/bin/flink run jobXXX.jar流程： 
   1. 读取配置文件，如果命令行指定了文件则从命令行读取，如果没有指定则读取$FLINK_HOME/conf目录的配置并生成org.apache.flink.configuration.Configuration对象保持到线程上下文中
   2. 新建线程classloader加载用户编写的jobXXX.jar
   3. 在上一步的线程中通过反射调用用户编写的main方法
   4. main方法中用户执行 StreamExecutionEnvironment.getExecutionEnvironment() 时会从线程上下文获取配置信息
   5. main方法中执行 StreamExecutionEnvironment.execute() 方法该方法在客户端生成StreamGraph 并进一步生成 JobGraph
   6. 将 JobGraph对象序列化并保持到临时文件 flink-jobgraphXXX.bin 中，同时将此文件和jobXXX.jar以及其他依赖的jar通过rest接口 post localhost:8081/v1/jobs 提交到服务器,参见: ClusterClient.submitJob()
    