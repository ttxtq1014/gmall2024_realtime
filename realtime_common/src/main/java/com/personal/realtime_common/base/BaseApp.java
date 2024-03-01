package com.personal.realtime_common.base;

/**
 * @author L
 * @description: TODO
 * @date 2024/2/29 16:15
 */

import com.personal.realtime_common.constant.Constant;
import com.personal.realtime_common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**TODO
 * TODO 这段代码定义了一个名为BaseApp的抽象类，该类包含了一个抽象方法和一个具体方法start。
 * 这个start方法用于设置和启动一个Flink流处理作业，该作业从Kafka中读取数据，并可能进行某种转换处理。
 * TODO 目的：这段代码的主要目的是提供一个基础框架，用于启动一个Flink流处理作业，该作业从Kafka读取字符串类型的数据流，并对数据流进行处理。
 * 它设置了一系列与作业执行和容错相关的配置，包括并行度、状态后端、检查点机制等，并封装了启动作业的流程。
 * 具体的数据处理逻辑（即handle方法）由继承自BaseApp的子类来实现。
 */

public abstract class BaseApp {
    //声明一个抽象方法handle,该方法由子类实现,用于处理数据流
    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream);

    //start方法用于启动Flink流处理作业,接收四个参数：端口号、并行度、检查点和组ID、kafka主题名
    public void start (int port, int parallelism, String ckAndGroupId, String topicName){
        //设置HDFS操作的用户名，通常用于权限控制
        System.setProperty("HADOOP_USER_NAME", Constant.HDFS_USER_NAME);
        //创建Hadoop的配置对象
        Configuration conf = new Configuration();

        //设置Flink作业与HDFS交互的端口号
        conf.setInteger("rest.port",port);
        //TODO 为什么要设置hdfs呢？
        //获取Flink流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //设置作业的并行度，即并行处理的任务数量
        env.setParallelism(parallelism);
        //设置状态后端为HashMapStateBackend,用于存储作业的状态信息
        env.setStateBackend(new HashMapStateBackend());

        //开启检查点机制，用于作业的容错和恢复
        env.enableCheckpointing(2000);//每2000ms进行一次检查点
        //获取检查点配置对象
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //设置检查点模式为精准一次(Exactly-Once),确保数据处理的语义正确性
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置检查点的存储位置在HDFS上
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/gmall2024/stream/" + ckAndGroupId);
        //设置并发检查点的最大数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        //设置两次检查点之间的最小暂停时间
        checkpointConfig.setMinPauseBetweenCheckpoints(5000);
        //设置检查点的超时时间
        checkpointConfig.setCheckpointTimeout(10000);
        //设置作业取消时检查点的保留策略为RETANIN_ON_CANCELLATION, 即保留检查点数据
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //准备Kafka数据源，调用FlinkSourceUtil工具类的getKafkaSource方法
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(ckAndGroupId, topicName);
        //从Kafka数据源读取数据，并设置不生成水位线(Watermark)
        DataStreamSource<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkasource");

        //调用抽象方法handle处理数据，该方法的实现由子类提供
        handle(env,stream);

        //启动FLink流处理作业
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


}
