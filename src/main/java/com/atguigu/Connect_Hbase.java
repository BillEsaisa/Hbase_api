package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

//单线程创建连接
public class Connect_Hbase {
    public static void main(String[] args) throws IOException {
        //创建连接配置对象
        Configuration conf = new Configuration();
        //设置conf
        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        //获取到连接
        Connection connection = ConnectionFactory.createConnection(conf);
        //打印连接
        System.out.println(connection);
        //关闭连接
        connection.close();


    }
}
