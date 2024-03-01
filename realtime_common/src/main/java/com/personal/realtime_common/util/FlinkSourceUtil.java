package com.personal.realtime_common.util;

import com.personal.realtime_common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author L
 * @description:
 * @date 2024/2/29 14:38
 */

/**
 * 和Kafka交互要用到Flink提供的KafkaSource及KafkaSink，Flink-1.16开始，
 * 原先用于交互的FlinkKafkaConsumer、FlinkKafkaProducer被标记为过时，不推荐使用。
 * 为了提高模板代码的复用性，将KafkaSource的构建封装到FlinkSourceUtil工具类中
 */
//定义一个方法getKafkaSource,该方法接收两个参数,GroupId和topicName,返回一个KafkaSource<String>对象
public class FlinkSourceUtil {
    public static KafkaSource<String> getKafkaSource(String groupId, String topicName){
        //创建一个kafkaSource<String> 对象，并设置其属性
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                //设置kafka服务器的地址，这里使用常量Constant.KAFKA_BROKERS来提供地址
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                //设置消费者组的ID，消费者组用于在多个消费者之间分配消息
                .setGroupId(groupId)
                //设置要监听的主题名称
                .setTopics(topicName)
                //设置值的反序列化方案，这里自定义了一个反序列化器，用于处理Kafka中的消息
                .setValueOnlyDeserializer(
                        //new SimpleStringSchema()
                        //存在问题，不适用，而是使用自定义的反序列化器
                        new DeserializationSchema<String>() {
                            //实现反序列化方法，将kafka中的字节消息转换为字符串
                            @Override
                            public String deserialize(byte[] message) throws IOException {
                                //如果消息不为空，则将其转换为字符串并返回
                                if (message != null) {
                                    return new String(message, StandardCharsets.UTF_8);
                                }
                                //如果消息为空，则返回空字符串
                                return "";
                            }

                            //实现判断流是否结束的方法，这里返回false,表示流没有结束
                            @Override
                            public boolean isEndOfStream(String nextElement) {
                                return false;
                            }

                            //返回反序列化后的数据类型信息，这里是字符串类型
                            @Override
                            public TypeInformation<String> getProducedType() {
                                return BasicTypeInfo.STRING_TYPE_INFO;
                            }
                        }
                )
                //设置从最早的偏移量开始读取消息，这通常用于测试，确保每次运行都能读取到所有的消息，而不用每次都重新模拟生成数据
                .setStartingOffsets(OffsetsInitializer.earliest())
                //设置消费者的隔离级别为“read_committed”
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")

                //完成kafkaSource对象的配置，并返回该对象
                .build();
        //返回配置好的kafkaSource对象
        return kafkaSource;

    }


    public static MySqlSource<String> getMysqlSource(String databaseName, String tableName){
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .databaseList(databaseName)
                .tableList(databaseName + "." + tableName)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        return mySqlSource;
    }
}
