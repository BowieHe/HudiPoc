# 使用说明

## 主程序

1. 编译代码
```shell script
./gradlew release
```
编译后的Jar包在build/libs/下面

2. 复制Jar包到可运行spark-submit的主机，运行如下命令启动数据填充任务
```shell script
spark-submit --master yarn --num-executors 4 --executor-memory 16G --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" --conf "spark.sql.hive.convertMetastoreParquet=false" --jars /usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar,/tmp/jars/spark-sql-kafka-0-10_2.12-3.1.2.jar,/tmp/jars/kafka-clients-2.6.2.jar,/tmp/jars/spark-token-provider-kafka-0-10_2.12-3.1.2.jar,/tmp/jars/commons-pool2-2.6.2.jar --class com.convertlab.data.CustomerDataInit /tmp/spark-data-filler-1.0-SNAPSHOT.jar
```

3. 数据写入的地址在
`s3://dev-hudipoc-emr-logs/tmp/hudi/poc/$tableName` 

4. 代码上传环境
`scp scp /Users/bowie/Desktop/HudiPoc/build/libs/HudiPoc-1.0-SNAPSHOT.jar ssh ec2-user@ec2-161-189-13-213.cn-northwest-1.compute.amazonaws.com.cn:/tmp`

`scp /tmp/HudiPoc-1.0-SNAPSHOT.jar hadoop@ip-172-20-105-156.cn-northwest-1.compute.internal:/tmp`