package com.adbibo.hive;

/**
 * Created by adbibo on 2017/2/15.
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Hive {

    private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static final String url = "jdbc:hive2://103.235.226.194:10000/test";
    private static final String userName = "";
    private static final String passWord = "";

//    private static final String dbName = "";
    private static final String tableName = "test";

    public static void check() {
        try {
            Class.forName(driverName);
            Connection con = DriverManager.getConnection(url, userName, passWord);
            Statement stmt = con.createStatement();
            // 如果存在了就删除
            String sql = "select * FROM " + tableName;
            ResultSet res = stmt.executeQuery(sql);
            int cnt = 0;
            while(res.next() && cnt < 20){
                System.out.println(res.getString(1)+"\t"+res.getString(2));
                cnt ++;
            }

        } catch (ClassNotFoundException e) {
            System.out.println("没有找到驱动类");
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("连接Hive的信息有问题");
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        check();
    }
}
