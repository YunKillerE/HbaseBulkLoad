package nova.main;

import nova.untils.HBaseUntils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Created by yunchen on 2017/3/29.
 */
public class testmain {

    public static void main(String[] args) throws IOException {

        Configuration hadoopconf = new Configuration();
        Connection connection = ConnectionFactory.createConnection(hadoopconf);
        Admin admin = connection.getAdmin();

        //表创建测试
        HBaseUntils hu = new HBaseUntils(admin,connection);
        hu.testcreatetable();
        hu.close();

    }


}
