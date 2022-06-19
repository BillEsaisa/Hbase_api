package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultithConnect_Hbase {
    //创建静态属性的连接
    public  static Connection connection =null;
    //静态代码块中获取HBASE连接
     static {

         //创建conf对象
        Configuration conf = new Configuration();

        //设置conf配置
        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");

        //返回连接

        try {
            connection  = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

     }
    // 关闭连接方法
     public static void closeconnection() throws IOException {
         if (connection != null){
             //关闭连接
             connection.close();
         }
     }

     //插入cell方法
    public static void putcell(String namespace ,String tablename ,String Rowkey ,String colmnfamily,String colmn ,String value) throws IOException {
         //先获取表
        TableName tableName = TableName.valueOf(namespace,tablename);
        Table table = connection.getTable(tableName);
        //创建put对象
        Put put = new Put(Bytes.toBytes(Rowkey));
        //创建put对象的属性
        put.addColumn(Bytes.toBytes(colmnfamily),Bytes.toBytes(colmn),Bytes.toBytes(value));
        //往表里put数据
        table.put(put);
        //关闭资源
       table.close();

    }
    //查询数据

    public static String getcell(String namespace,String tablename, String rowkey,String colmnfamily,String colmn) throws IOException {
         //先获取连接，库，表
        Table table = connection.getTable(TableName.valueOf(namespace, tablename));
        //创建get对象
        Get get = new Get(Bytes.toBytes(rowkey));
        //为get对象添加属性
        get.addColumn(Bytes.toBytes(colmnfamily),Bytes.toBytes(colmn));
        //get对象
        byte[] bytes = table.get(get).value();
        String s = new String(bytes);
        //关闭资源
        table.close();
        //返回get结果
        return s;


    }


    //扫描数据
    public static @NotNull List<String> scanrow(String namespace , String tablename , String startrow , String stoprow) throws IOException {
         //获取表
        Table table = connection.getTable(TableName.valueOf(namespace, tablename));
        //创建扫描对象scan
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startrow));
        scan.withStopRow(Bytes.toBytes(stoprow));
        //创建容器
        ArrayList<String> list = new ArrayList<>();
        //扫描数据
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            list.add(Bytes.toString(result.value()));
        }
        //关闭资源
        scanner.close();
        table.close();
        //返回数据
        return list;

    }


    //删除数据
    public  static void deletecolmn(String namespace, String tablename,String rowkey ,String colmnfamily,String colmn) throws IOException {
         //获取表
        Table table = connection.getTable(TableName.valueOf(namespace, tablename));
        //创建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        //为删除对象添加属性(删除单个版本)
        delete.addColumn(Bytes.toBytes(colmnfamily),Bytes.toBytes(colmn));
        //删除数据
        table.delete(delete);
        //关闭资源
        table.close();

    }

    public static void main(String[] args) throws IOException {
        //putcell("default","stu","1010","info","name","ooo");
        String cell = getcell("default", "stu", "1010", "info", "name");
        System.out.println(cell);
    }









}
