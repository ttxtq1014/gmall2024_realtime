package com.personal.realtime_common.util;

import com.alibaba.fastjson.JSONObject;
import com.personal.realtime_common.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import javax.ws.rs.PUT;
import java.io.IOException;
import java.util.Set;

/**
 * @author L
 * @description: TODO
 * @date 2024/3/1 9:58
 */
@Slf4j
public class HBaseUtil {
    //获取HBase连接
    public static Connection getConnection() throws Exception {
        //创建HBase配置对象
        Configuration configuration = new Configuration();
        //设置Zookeeper服务地址
        configuration.set("hbase.zookeeper.quorum", Constant.HBASE_ZOOKEEPER_QUORUM);
        //创建HBase连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }
    //关闭HBase连接
    public static void closeConnection(Connection connection) throws Exception {
        //检查连接是否存在且未关闭，然后关闭连接
        if (connection != null && !connection.isClosed()){
            connection.close();
        }
    }

    //TODO 创建HBase表
    public static void createTable(Connection connection , String namespaceName, String tableName, String ... cfs) throws IOException {
        //至少一个列族
        if (cfs == null || cfs.length < 1) {
            log.warn("创建" + namespaceName + ":" + tableName + ",至少指定一个列族");
            return;
        }
        //获取Admin对象
        Admin admin = connection.getAdmin();
        //构建表名
        TableName tn = TableName.valueOf(namespaceName, tableName);
        //判断表是否已经存在
        if (admin.tableExists(tn)) {
            log.warn("创建" + namespaceName + ":" + tableName + ",表已经存在！！！");
            return;
        }
        //构建表描述器
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tn);
        //遍历所有列族,添加到表描述器中
        for (String cf : cfs) {
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
            ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        }
        //构建表描述对象
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        //创建表
        admin.createTable(tableDescriptor);
        //关闭Admin连接
        admin.close();
        log.info("创建" + namespaceName + ":" + tableName + ",成功！！！");
    }

    //TODO 删除HBase表
    public static void dropTable(Connection connection, String namespaceName, String tableName) throws IOException {
        //获取Admin对象
        Admin admin = connection.getAdmin();
        //构建表名
        TableName tn = TableName.valueOf(namespaceName, tableName);
        //判断表是否存在
        if (!admin.tableExists(tn)) {
            log.warn("删除" + namespaceName + ":" + tableName + ",表不存在！！！");
            return;
        }
        //禁用表
        admin.disableTable(tn);
        //删除表
        admin.deleteTable(tn);
        //关闭Admin连接
        admin.close();
        log.info("删除" + namespaceName + ":" + tableName + ",成功！！！");
    }


    //TODO 写入数据
    public static void putCells(Connection connection, String namespaceName, String tableName, String rowkey, String cf, JSONObject dataObj) throws IOException {
        //通过对象，获取表
        TableName tn = TableName.valueOf(namespaceName, tableName);
        Table table = connection.getTable(tn);

        //写入数据
        Put put = new Put(Bytes.toBytes(rowkey));
        Set<String> allColumns = dataObj.keySet();
        for (String column : allColumns) {
            String value = dataObj.getString(column);
            if (value != null) {
                put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
            }
        }
        table.put(put);
        table.close();
        log.info("写入" + rowkey + "数据到" + namespaceName + ":" + tableName);
    }

    //TODO 删除数据
    public static void deleteCells(Connection connection, String namespaceName, String tableName, String rowKey) throws IOException {
        TableName tn = TableName.valueOf(namespaceName, tableName);
        Table table = connection.getTable(tn);
        //删除数据
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        table.close();

        log.info("删除" + rowKey + "数据从" + namespaceName + ":" + tableName);
    }
}
