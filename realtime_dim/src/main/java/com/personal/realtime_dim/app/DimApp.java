package com.personal.realtime_dim.app;

import base.BaseApp;
import bean.TableProcessDim;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.personal.realtime_common.util.HBaseUtil;
import com.personal.realtime_common.util.RedisUtil;
import constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;
import util.FlinkSourceUtil;


import com.personal.realtime_common.util.JdbcUtil;


import java.util.*;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * @author L
 * @description: TODO
 * @date 2024/2/29 20:18
 */
@Slf4j
public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(10001,4,"dim_app", Constant.TOPIC_DB);
    }

    //每个app,具体关注核心逻辑的实现
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //核心逻辑处理
        //TODO 1.读取ODS层的topic_db数据
        //在start()中已经完成

        //TODO 2.对读取的数据进行清洗过滤
        /**
         * 这段代码是Apache Flink流处理中的一个flatMap操作，用于处理输入的数据流。具体来说，它接收一个字符串（假设是JSON格式）作为输入，
         * 然后对这个字符串进行解析和过滤，最后输出满足特定条件的JSON对象。
         * 主要作用是过滤和转换输入的数据流。它首先尝试将每个输入字符串解析为JSON对象，然后根据一系列条件判断该对象是否应该被包含在输出流中。
         * 如果对象满足条件，它将被收集到输出流中；否则，它将被过滤掉，并且会记录一条警告日志。
         */
        SingleOutputStreamOperator<JSONObject> etlStream = stream.flatMap(
                //创建一个新的flatMapFunction实例,输入类型为String,输出类型为JSONObject
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        //尝试将输入的字符串(假设是JSON格式),解析为JSONObject对象
                        JSONObject jsonObject = JSON.parseObject(value);
                        //从解析后的JSONObject中提取"database"字段的值
                        String database = jsonObject.getString("database");
                        //从解析后的JSONObject中提取"type"字段的值
                        String type = jsonObject.getString("type");
                        //从解析后的JSONObject中提取"data"字段的值,该值也是一个JSONObject
                        JSONObject dataObj = jsonObject.getJSONObject("data");
                        /**
                         * 判断条件是否满足：
                         * 1.数据库名为"gmall"
                         * 2.类型不是"bootstrap-start"和"bootstrap-complete"
                         * 3."data"字段存在且不为空
                         */
                        if ("gmall".equals(database)
                                && !"bootstrap-start".equals(type)
                                && !"bootstrap-complete".equals(type)
                                && dataObj != null
                                && dataObj.size() > 0) {
                            //如果以上条件都满足,则将该JSONObject对象收集到输出流中
                            out.collect(jsonObject);
                        }
                        //如果在解析过程中出现异常,则记录一条警告日志,并过滤掉这条脏数据
                        log.warn("过滤掉脏数据：" + value);
                    }
                }
        );
        //etlStream.print("etl");

        //TODO 3.读取配置表中的数据,动态处理HBase中的维度表
        /**
         * 方式四:  将维度表  以配置表的形式  写入到 mysql表中，  表中数据有变化， 代码能需要实时感知并做出对应的处理
         *                                      ==> maxwell ==> kafka topic  ==> flink ==>  stream
         *                     mysql    binlog
         *                                      ==> TODO flink cdc ==>  stream
         */
        /**
         * 这段代码是使用 Apache Flink 的 DataStream API 来从 MySQL 数据库中读取数据，并对这些数据进行转换处理。
         * 简而言之，这段代码从MySQL数据库中读取数据，将读取到的JSON字符串解析为TableProcessDim对象，
         * 并根据操作类型（如创建、读取、更新或删除）来提取不同的字段数据。
         * 最后，这个处理后的数据流被设置为并行度为1，意味着所有的处理都在一个任务中执行。
         */
        //从MySql数据源创建一个DataStream,不生成watermark,并将此DataStream命名为"mysqlsource"
        DataStreamSource<String> configStream =
                env.fromSource(FlinkSourceUtil.getMysqlSource(Constant.TABLE_PROCESS_DATABASE, Constant.TABLE_PROCESS_DIM), WatermarkStrategy.noWatermarks(), "mysqlsource");

        //创建一个新的DataStream,名为tableProcessDimStream,该Stream中的元素类型为TableProcessDim
        SingleOutputStreamOperator<TableProcessDim> tableProcessDimStream = configStream.map(
                new MapFunction<String, TableProcessDim>() {
                    //重写map 方法，将输入的String类型数据转换为TableProcessDim
                    @Override
                    public TableProcessDim map(String value) throws Exception {
                        //将输入的字符串解析为JSON对象
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        //从JSON对象中提取操作类型(c:create,r:read,u:update,d:delete)
                        String op = jsonObject.getString("op");
                        TableProcessDim tableProcessDim;
                        //根据操作类型判断从JSON对象中提取哪个字段的数据来创建TabelProcessDim对象
                        if ("d".equals(op)) {
                            //如果是delete操作,则从"before"字段中提取数据
                            tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                        } else {
                            //如果是创建、读取、更新操作，则从"after"字段中提取数据
                            //c r u
                            tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                        }
                        //在TableProcessDim对象中设置操作类型
                        tableProcessDim.setOp(op);

                        //返回转换后的TableProcessDim对象
                        return tableProcessDim;
                    }
                }
                //设置并行度为1，只在一个任务中执行 TODO 为什么设置为1？
        ).setParallelism(1);

        //tableProcessDimStream.print("tableProcessDimStream");

        //TODO 4.在HBase中建表或删表
        /**
         * 这段代码是使用Apache Flink的DataStream API来执行基于HBase的操作。
         * 具体来说，它根据传入的TableProcessDim对象中的操作类型（op字段）来决定是创建还是删除HBase表。
         * 这段代码的主要功能是：
         * 1.使用RichMapFunction来管理HBase连接的生命周期，确保在函数执行前后正确地打开和关闭连接。
         * 2.在map方法中，根据TableProcessDim对象中的op字段值来决定是创建表、删除表还是同时执行两者。
         * 3.createHBaseTable和dropHBaseTable是两个私有辅助方法，分别用于创建和删除HBase表。
         * 4.最后，设置DataStream的并行度为1，确保所有的操作都在一个任务中顺序执行，并且返回处理后的DataStream。
         * 需要注意的是，通常在使用Flink处理这种有状态的操作（如创建或删除表）时，会考虑使用更细粒度的控制来管理并行度和状态，
         * 以避免单点故障或性能瓶颈。
         * 此外，返回值TableProcessDim可能在这个场景下没有实际用途，因为主要的操作是基于副作用（即HBase表的创建和删除）进行的。
         */
        //定义一个RichMapFunction,它继承自MapFunction,并增加了生命周期方法(open和close)
        //用来管理HBase的连接
        SingleOutputStreamOperator<TableProcessDim> createOrDropTableStream = tableProcessDimStream.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    //定义HBase连接对象
                    Connection connection;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //在RichMapFunction的open方法中初始化HBase连接
                        connection = HBaseUtil.getConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        //在RicehMapFunction的open方法中关闭HBase连接
                        HBaseUtil.closeConnection(connection);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tableProcessDim) throws Exception {
                        //获取TableProcessDim对象中的操作类型
                        String op = tableProcessDim.getOp();
                        //根据操作类型决定是删除表、创建表还是同时指定两者
                        if ("d".equals(op)) {
                            //如果操作类型为删除，则调用dropHBaseTable方法删除表
                            dropHBaseTable(tableProcessDim);
                        } else if ("u".equals(op)) {
                            //如果操作类型是更新，则是先删除表再创建表
                            dropHBaseTable(tableProcessDim);
                            createHBaseTable(tableProcessDim);
                        } else {
                            //对于创建和读取操作(c 和 r),只执行创建表的操作
                            createHBaseTable(tableProcessDim);
                        }
                        //返回处理后的TableProcessDim对象，(实际上在这个场景中可能不需要返回，因为操作是基于副作用的)
                        return tableProcessDim;
                    }

                    //私有方法，用于创建HBase表
                    private void createHBaseTable(TableProcessDim tableProcessDim) throws IOException {
                        //从TableProcessDim对象中过去列族信息，并将其拆分为数组
                        String[] cfs = tableProcessDim.getSinkFamily().split(",");
                        //使用HBaseUtil工具类来创建表，指定连接、命名空间、表名、列族信息
                        HBaseUtil.createTable(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), cfs);
                    }

                    //私有方法，用于删除HBase表
                    private void dropHBaseTable(TableProcessDim tableProcessDim) throws IOException {
                        //使用HBaseUtil工具类来删除表，指定连接、命名空间、表名、列族信息
                        HBaseUtil.dropTable(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable());
                    }

                }
        ).setParallelism(1);//设置并行度为1，意味着它只在一个任务中执行。
        //createOrDropTableStream.print("createOrdropTable");

        //TODO 5.将配置流处理成广播流
        /**
         * 这段代码的功能是将一个DataStream转换成一个BroadcastStream，同时为这个BroadcastStream定义一个MapStateDescriptor，
         * 用于在Flink作业中广播状态,
         * 这段代码演示了如何在Flink中创建一个广播流，并将一个DataStream转换成该广播流。
         * 这样做的好处是，其他需要这些配置信息或静态数据的DataStream可以高效地访问这些数据，而不需要在每个并行任务中都复制这些数据。
         * 通过广播流，这些数据只会被发送一次，然后会被所有的并行任务共享。这在
         * 处理大量数据或需要频繁访问相同配置的场景下非常有用。
         */
        //创建一个MapStateDescriptor，用于描述要广播的状态
        //这个状态是一个Map，键是string 类型，值是TableProcessDim类型
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
                new MapStateDescriptor<>("mapStateDesc", Types.STRING, Types.POJO(TableProcessDim.class));
        //使用broadcast方法将原来的DataStream(createOrDropTableStream)转换成一个BroadcastStream
        //以便允许其他DataStream以广播的方式访问这个流中的状态数据,通常用于全局配置或静态数据的分发
        BroadcastStream<TableProcessDim> broadcastStream = createOrDropTableStream.broadcast(mapStateDescriptor);

        //TODO 6.处理topic_db中的维度数据
        /**
         * 它使用了Broadcast Process Function来连接一个普通数据流（etlStream）和一个广播流（broadcastStream）。
         * Broadcast Process Function允许你处理两个输入流，其中一个输入流（这里是broadcastStream）会被广播到所有并行任务中，
         * 而另一个输入流（这里是etlStream）则按正常方式处理。
         * BroadcastProcessFunction的processElement方法的实现，它是流处理的核心部分，用于处理每个从etlStream输入的数据元素。
         * 在Flink中，processElement方法会在每个数据元素上被调用，并允许你根据输入数据和广播状态来生成输出。
         * BroadcastProcessFunction的processBroadcastElement方法的实现。
         * 在Flink流处理中，BroadcastProcessFunction允许你处理广播流（broadcast stream）中的元素，
         * 这些元素会被发送到每个并行任务（task）中，用于更新某些状态或配置信息。
         * processBroadcastElement方法会在每个广播元素到达时被调用。
         */
        //连接广播流和普通数据流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream = etlStream.connect(broadcastStream)
                .process(
                        //使用BroadcastProcessFunction处理两个流的数据
                        new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {
                            //基于生命周期open方法会早于processElement方法执行，
                            //可以在该方法中预加载配置表数据，存入到一个集合中 , 配合状态来使用
                            //Flink CDC 初始启动会比较慢， 主流数据早于配置流， 主流中本应该定性为维度表的数据 ， 因为状态中还没有存入维度表信息，
                            //而最终定性为非维度表数据

                            //定义一个Map用于存储预加载的配置表数据
                            private Map<String, TableProcessDim> preConfigMap;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                //建立数据库连接
                                java.sql.Connection connection = JdbcUtil.getConnection();
                                //从数据库中查询配置表数据
                                List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(
                                        connection,
                                        "select `source_table`, `sink_table`, `sink_family`, `sink_columns`, `sink_row_key` from gmall2024_config.table_process_dim",
                                        TableProcessDim.class,
                                        true);
                                //为了方便根据source_table查询配置，将查询结果转换成map结构
                                preConfigMap = new HashMap<>();

                                for (TableProcessDim tableProcessDim : tableProcessDimList) {
                                    preConfigMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
                                }
                                //关闭数据库连接
                                JdbcUtil.closeConnection(connection);
                            }

                            @Override
                            public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
                                //获取广播状态，即之前从广播流中加载的配置信息
                                ReadOnlyBroadcastState<String, TableProcessDim> readOnlyBroadcastState = ctx.getBroadcastState(mapStateDescriptor);
                                //从输入的JSON对象中获取表名
                                String tableName = jsonObject.getString("table");
                                //尝试从广播状态中根据表名获取配置信息
                                TableProcessDim tableProcessDim = readOnlyBroadcastState.get(tableName);
                                //如果从广播状态中获取的tableProcessDim为空，则尝试从预加载的Map中读取
                                if (tableProcessDim == null) {
                                    tableProcessDim = preConfigMap.get(tableName);
                                    log.info("从预加载的Map中读取维度信息");
                                }
                                //如果成功获取到tableProcessDim，(无论是从广播状态还是预加载的Map中),则将其与输入的JSON对象一起输出
                                if (tableProcessDim != null) {
                                    //使用Tuple2将JSONOject和TableProcessDim组合在一起，并通过Collector输出
                                    out.collect(Tuple2.of(jsonObject, tableProcessDim));
                                }
                            }

                            @Override
                            public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
                                System.out.println("DimApp.ProcessBroadcastElement......................");
                                //获取广播状态，这是一个用于存储维度表信息的状态
                                BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                                //从传入的TableProcessDim对象中获取操作类型
                                String op = tableProcessDim.getOp();
                                //根据操作类型决定如何处理
                                if ("d".equals(op)) {
                                    //删除维度表并从广播状态中移除对应的维度表
                                    broadcastState.remove(tableProcessDim.getSourceTable());
                                } else {
                                    //反之，操作类型则为c r u 或者其他 并将 维度表添加到广播状态中
                                    broadcastState.put(tableProcessDim.getSourceTable(), tableProcessDim);
                                }
                            }
                        }
                ).setParallelism(1);
        //dimStream.print("dim");

        //TODO 7.删除不需要的字段
        /**
         * 这段代码的主要作用是过滤掉JSONObject中data字段下不需要的键值对，
         * 只保留那些在TableProcessDim的sinkColumns字段中明确指定的字段。
         * 这段代码的目的是对dimStream中的每个Tuple2<JSONObject, TableProcessDim>元素进行处理。
         * 它首先从Tuple2中提取JSONObject和TableProcessDim对象，然后从JSONObject中进一步提取名为data的子对象，
         * 并获取该子对象中所有的键。接着，它根据TableProcessDim对象中的sinkColumns字段来确定哪些键是需要保留的。
         * 最后，它过滤掉data对象中不需要的键，并返回处理后的Tuple2对象。
         */
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterSinkColumnsStream = dimStream.map(
                new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
                    @Override
                    public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> value) throws Exception {
                        //获取对象
                        JSONObject jsonObject = value.f0;
                        TableProcessDim tableProcessDim = value.f1;

                        //从jsonObject中提取名为"data"的JSONObject
                        JSONObject dataObj = jsonObject.getJSONObject("data");
                        //获取dataObj中所有的键，并存储到Set中
                        Set<String> dataAllKeys = dataObj.keySet();
                        //从tableProcessDim中获取sinkColumns字符串，该字符串包含了需要保留的字段名
                        String sinkColumnsList = tableProcessDim.getSinkColumns();

                        //使用removeif方法过滤掉dataAllKeys中不在sinkColumns中的键
                        //这意味着我们仅保留那些在sinkColumns中明确指定的字段
                        dataAllKeys.removeIf(key -> !sinkColumnsList.contains(key));

                        return Tuple2.of(jsonObject, tableProcessDim);

                    }
                }
        );
        //filterSinkColumnsStream.print("过滤");

        //TODO 8.将数据写出到Hbase的表中
        filterSinkColumnsStream.addSink(
                new RichSinkFunction<Tuple2<JSONObject, TableProcessDim>>() {
                    Connection connection;
                    Jedis jedis;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //获取Hbase的连接
                        connection = HBaseUtil.getConnection();
                        jedis = RedisUtil.getJedis();
                    }

                    @Override
                    public void close() throws Exception {
                        //关闭Hbase的connection
                        HBaseUtil.closeConnection(connection);
                        RedisUtil.closeJedis(jedis);
                    }

                    @Override
                    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
                        //将维度表的数据写入到Hbase中对应的维度表中，
                        JSONObject jsonObject = value.f0;
                        TableProcessDim tableProcessDim = value.f1;
                        //写出的数据
                        JSONObject dataObj = jsonObject.getJSONObject("data");
                        //数据的操作类型
                        String type = jsonObject.getString("type");
                        //根据类型，决定是put 还是 delete
                        if ("delete".equals(type)) {
                            //从hbase的维度表中删除维度数据
                            deleteDimData(dataObj, tableProcessDim);
                        } else {
                            //insert update bootstrap-insert
                            putDimData(dataObj,tableProcessDim);
                        }
                        //补充：保证Hbase中的维度数据和Redis缓存的数据保持一致性
                        //当业务数据库中的维度数据发生变化后，会对应修改Hbase中的维度数据
                        //对于redis缓存中的维度数据，需要执行删除操作
                        if ("update".equals(type) || "delete".equals(type)) {
                            RedisUtil.deleteDim(jedis, Constant.HBASE_NAMESPACE,tableProcessDim.getSinkTable(),dataObj.getString(tableProcessDim.getSinkRowKey()));
                        }
                    }

                    private void deleteDimData(JSONObject dataObj, TableProcessDim tableProcessDim) throws IOException {
                        //rowKey
                        String sinkRowKeyName = tableProcessDim.getSinkRowKey();
                        String sinkRowKeyValue = dataObj.getString(sinkRowKeyName);
                        HBaseUtil.deleteCells(connection,Constant.HBASE_NAMESPACE,tableProcessDim.getSinkTable(),sinkRowKeyValue);
                    }

                    private void putDimData(JSONObject dataObj, TableProcessDim tableProcessDim) throws IOException {
                        //rowKey
                        String sinkRowKeyName = tableProcessDim.getSinkRowKey();
                        String sinkRowKeyValue = dataObj.getString(sinkRowKeyName);
                        //cf
                        String sinkFamily = tableProcessDim.getSinkFamily();
                        HBaseUtil.putCells(connection, Constant.HBASE_NAMESPACE,tableProcessDim.getSinkTable(),sinkRowKeyValue,sinkFamily,dataObj);
                    }
                }
        );

    }
}
