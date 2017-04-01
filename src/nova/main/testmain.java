package nova.main;

import nova.untils.HBaseUntils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
/**
 * Created by yunchen on 2017/3/29.
 */
public class testmain {

    public static void main(String[] args) throws IOException {

/*
        //测试建表
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        //表创建测试
        HBaseUntils hu = new HBaseUntils(admin,connection);
        hu.createTableBySplitKeys("ttee",new String[]{"info","f1","f2"},"SNAPPY",10,"partition");
        hu.close();

*/

        //测试随机数的输出
/*        HashChoreWoker worker = new HashChoreWoker(100,10);
        byte [][] splitKeys = worker.calcSplitKeys();

        for(int i = 0;i<splitKeys.length;i++){
            System.out.println(Arrays.toString(splitKeys[i]));
        }*/

/*        String cols  = "info,f1,f2,f3";

        String[] cc = cols.split(",");

        for(String c:cc) {
            System.out.println(c);
        }*/


        String num = "1";
        String[] numarr = num.split(",");
        if (numarr.length == 1) {
            System.out.println("长度为1==========" + numarr[0]);
        } else {
            System.out.println("长度为:" + numarr.length);
            for (int i = 0; i < numarr.length; i++) {
                System.out.println("第一个" + i + "的值为:" + numarr[i]);
            }
        }
    }

}
