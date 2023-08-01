# 组件测试
这里测试通过编程的方式启动flink各个组件

1. org.huang.flink.entrypoint.SessionClusterEntrypointApp 启动一个jobmanager， 还包括dispacher和ResourceManager
2. org.huang.flink.entrypoint.TaskManagerRunnerApp 启动一个taskmanager
3. org.huang.flink.entrypoint.CliFrontendApp 解析一个flinkjar包并其提交到jobmanager