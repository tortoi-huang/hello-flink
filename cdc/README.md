# hello-flink
读取 mysql binlog   
运行gradle 任务 shadowJar 打包关联 jar
```shell
# ./gradlew :common:package :cdc:shadowJar
# .\gradlew.bat :cdc:clean :cdc:shadowJar
./gradlew :cdc:clean :cdc:shadowJar
```
## 启动 mysql
```shell
# 启动数据库
sudo docker compose -f docker/docker-compose.yaml up -d
# sudo docker compose -f docker/docker-compose.yaml down -v
```

## 启动 flink 任务
观察到控制台输出了 person表的当前快照, 而不是所有的历史 binlog

## 插入数据到 mysql测试
从 mysql 插入数据查看flink 控制台输出结果
```shell
sudo docker exec -it mysql1 mariadb -uroot -pMypass@112233 test -e "update person set first_name='111' where person_no='3';"
sudo docker exec -it mysql1 mariadb -uroot -pMypass@112233 test -e "update person set first_name='222' where person_no='3';"

# 重启 flink 观察控制台， 只有3条新增的最新的bin log 输出
# 修改数据  观察flink 控制台有binlog日志输出
sudo docker exec -it mysql1 mariadb -uroot -pMypass@112233 test -e "update person set first_name='aaa' where person_no='3'; "
```

## 禁用 checkpoints 重启flink
发现与设置了checkpoints 一致,启动前的数据都表现为一个新增的binlog, 启动后会记录改变的记录

## 总结
+ 每次重新部署任job时会给任务生成一个随机的 job id, 可以通过configuration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID,"b7a2c8ba2d80390c4ee5e288fe93a994") 来指定id 
+ 每次 jobmanager 重启需要指定fink checkpoint, 官方没有自动使用上次checkpoint的实现, 哪怕job id一样要指定 
+ gradle shadow 插件可以打包 fat jar, 默认会打包所有依赖. 使用 include 指令后则只打包 使用 include 的依赖, 并不打包依赖的依赖, 需要逐个找出依赖树
