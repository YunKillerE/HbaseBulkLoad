package nova.main;

import nova.MapUntils.MapBlukLoad;
import nova.untils.HBaseUntils;
import nova.untils.PropertiesUtils;
import org.apache.commons.el.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapred.TestMerge;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by yunchen on 2017/3/24.
 *
 *
 *  * 思路：
 * 1，rowkey的设计
 *      1）列中的某一个字段作为rwokey
 *      2）列中的某一个字段加上随机数作为rowkey
 *      3）多个列组合加随机数
 *      4）某个列的一部分加随机数
 * 2，列
 *      1）每一列作为一个列
 *      2）多个列组合作为一个列
 * 3，列族
 *      1）通过一个数组或者字符串获取所有数组
 *      2）需要事先定义好哪些列属于哪个列族
 *
 *
 * 工作计划：
 * 1，列族组合功能
 * 2，表创建功能，主要通过列族来创建表，这里涉及到rowkey散列和预分区问题，和rowkey的设计会相关
 * 3，rowkey拼接功能，根据前面列举的几中情况来写相应的函数
 * 4，列的组合功能，这里相对比较复杂，可能需要根据场景来
 *
 */


public class BulkLoadMR {

    public static void main(String[] args) throws Exception {


        //1，获取说需要的变量
        String config_path = args[0];

        String tableName = PropertiesUtils.get_NameValues(config_path,"tablename");
        String srcPath = PropertiesUtils.get_NameValues(config_path,"hdfs_src_path");
        String hFilePath = PropertiesUtils.get_NameValues(config_path,"hfile_outputdir");
        //String zkAddress = PropertiesUtils.get_NameValues(config_path,"hbase_zookeeper");
        //String hbase_master = PropertiesUtils.get_NameValues(config_path,"hbase_master");
        String hbase_site_path = PropertiesUtils.get_NameValues(config_path,"hbase_site_path");
        String hdfs_site_path = PropertiesUtils.get_NameValues(config_path,"hdfs_site_path");
        String columnField = PropertiesUtils.get_NameValues(config_path,"columnField");
        String columnFamily = PropertiesUtils.get_NameValues(config_path,"columnFamily");
        String compressType = PropertiesUtils.get_NameValues(config_path,"compressType");
        Integer regionSplitNum = Integer.valueOf(PropertiesUtils.get_NameValues(config_path,"regionSplitNum"));
        String splitType = PropertiesUtils.get_NameValues(config_path,"splitType");
        String rowkeyLine = PropertiesUtils.get_NameValues(config_path,"rowkeyLine");
        String issubstring = PropertiesUtils.get_NameValues(config_path,"issubstring");
        String substringbegin = PropertiesUtils.get_NameValues(config_path,"substringbegin");
        String substringend = PropertiesUtils.get_NameValues(config_path,"substringend");
        String conbinerowkey = PropertiesUtils.get_NameValues(config_path,"conbinerowkey");
        String HowNumLine = PropertiesUtils.get_NameValues(config_path,"HowNumLine");

        //2，创建hbase/hdfs conf
        Configuration hconf = new Configuration();
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path(hbase_site_path));
        hconf.addResource(new Path(hdfs_site_path));

        //3，创建hbase admin
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        FileSystem fs = FileSystem.get(hconf);

        //4，根据提供的表名和列族创建hbase表
        HBaseUntils hu = new HBaseUntils(admin,connection);
        String[] cols = columnFamily.split(",");//构建列族数组
        hu.createTableBySplitKeys(tableName,cols,compressType,regionSplitNum,splitType);

        //5，往map函数传递变量，主要有分隔符，rowkey，列
        conf.set("field",columnField);
        conf.set("rowkeyLine",rowkeyLine);
        conf.set("issubstring",issubstring);
        conf.set("substringbegin",substringbegin);
        conf.set("substringend",substringend);
        conf.set("HowNumLine",HowNumLine);
        conf.set("splitType",splitType);

        //6，生成hfile文件

        if(fs.exists(new Path(hFilePath))){
            fs.delete(new Path(hFilePath),true);
        }

        HTable table = new HTable(conf, tableName);
        table.setWriteBufferSize(6*1024*1024);
        Job job = new Job(conf);
        job.setJarByClass(BulkLoadMR.class);
        job.setMapperClass(MapBlukLoad.Map.class);//指向map class
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        FileInputFormat.addInputPath(job, new Path(srcPath));
        //TODO 路径如果存在直接删除，不过要是数据目录删除就麻烦了,可以考虑定义一个没有意义不会使用到的目录
        FileOutputFormat.setOutputPath(job, new Path(hFilePath));
        HFileOutputFormat2.configureIncrementalLoad(job, table.getTableDescriptor(), table.getRegionLocator());
        job.waitForCompletion(true);

        //7，将hfile导入到hbase的表中
        LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(conf);
        loadFfiles.doBulkLoad(new Path(hFilePath), admin, table, table.getRegionLocator());

        //8，关闭相关的连接
        hu.close();
        fs.close();


    }


}
