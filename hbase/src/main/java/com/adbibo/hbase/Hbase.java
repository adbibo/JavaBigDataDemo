package com.adbibo.hbase;

/**
 * Created by adbibo on 2017/2/15.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


/**
 * Created by adbibo on 2017/02/15.
 */
public class Hbase {
    private static final String TABLE_NAME = "myuser";
    private static final String[] CF_DEFAULT = {"info"};

    public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        TableName tableName = table.getTableName();

        if (admin.isTableDisabled(tableName)) {
            //如果表不可用，先置为可用
            admin.enableTable(tableName);
        }
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            System.out.print("Table exists, delete it first... ");
            admin.deleteTable(tableName);
            System.out.print("Table deleted, Done. ");
        }
        admin.createTable(table);
    }

    public static void createSchemaTables(Configuration config) throws IOException {
        try {

            Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin();

            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));

            for (int i = 0; i < CF_DEFAULT.length; i++) {
                String family = CF_DEFAULT[i];
                table.addFamily(new HColumnDescriptor(family).setCompressionType(Compression.Algorithm.NONE));
            }

            System.out.print("Creating table. ");
            createOrOverwrite(admin, table);
            System.out.println(" Done.");
        } catch (Exception e) {
            System.out.println("Creating table failure：" + e.getMessage());
            System.out.println("Creating table failure：" + e.getStackTrace());
            System.out.println("Creating table failure：" + e.getCause());
        }
    }

    public static void addData(Configuration config, String rowKey,
                               String[] column1, String[] value1, String[] column2, String[] value2)
            throws IOException {

        Connection connection = ConnectionFactory.createConnection(config);
        try {
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

            // 获取表
            HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies(); // 获取所有的列族
            Put put = new Put(Bytes.toBytes(rowKey));


            for (int i = 0; i < columnFamilies.length; i++) {
                String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
                if (familyName.equals("article")) { // article列族put数据
                    for (int j = 0; j < column1.length; j++) {
                        put.addColumn(Bytes.toBytes(familyName),
                                Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                    }
                }
                if (familyName.equals("author")) { // author列族put数据
                    for (int j = 0; j < column2.length; j++) {
                        put.addColumn(Bytes.toBytes(familyName),
                                Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
                    }
                }
            }
            table.put(put);

        } finally {
            connection.close();
        }
        System.out.println("add data Success!");
    }


    public static void modifySchema(Configuration config) throws IOException {
        try {

            Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin();

            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tableName)) {
                System.out.println("Table does not exist.");
                System.exit(-1);
            }

            HTableDescriptor table = admin.getTableDescriptor(tableName);

            // Update existing table
            HColumnDescriptor newColumn = new HColumnDescriptor("name");
            newColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
            newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            admin.addColumn(tableName, newColumn);

            // Update existing column family
            for (int i = 0; i < CF_DEFAULT.length; i++) {
                String family = CF_DEFAULT[i];
                HColumnDescriptor existingColumn = new HColumnDescriptor(family);
                existingColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
                existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
                table.modifyFamily(existingColumn);
            }

            admin.modifyTable(tableName, table);

            // Disable an existing table
            admin.disableTable(tableName);

            // Delete an existing column family
//            admin.deleteColumn(tableName, CF_DEFAULT.getBytes("UTF-8"));

            // Delete a table (Need to be disabled first)
            admin.deleteTable(tableName);
        } catch (Exception e) {
            System.out.println("modifySchema failure：" + e.getMessage());
            System.out.println("modifySchema failure：" + e.getStackTrace());
            System.out.println("modifySchema failure：" + e.getCause());
        }
    }

    public static void main(String[] args) throws IOException {

        Configuration config = HBaseConfiguration.create();

        config.set("hbase.ZooKeeper.quorum", "namenode1");  //hbase 服务地址
        config.set("hbase.ZooKeeper.property.clientPort", "2181"); //端口号

        createSchemaTables(config);
    }
}