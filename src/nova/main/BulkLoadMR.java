package nova.main;

import nova.MapUntils.MapBlukLoad;
import nova.untils.HBaseUntils;
import nova.untils.PropertiesUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by yunchen on 2017/3/24.
 *
 * 原理：
 *
 * 1，通过 MapReduce 任务生成 HFile
 *
 * 在进行数据导入时，需要对数据进行预处理，如过滤无效数据、数据格式转换等。通常按照不同的导入要求，
 * 需要编写不同的 Mapper；Reducer 由 HBase 负责处理。为了按照 HBase 内部存储格式生成数据，一个重要的类是
 * HFileOutputFormat2(HBase 1.0.0以前版本使用 HFileOutputFormat)。为了更有效地导入数据，
 * 每一个输出的 HFile 要恰好适应一个 Region。为了确保这一点， 需要使用 TotalOrderPartitioner 类将 map
 * 的输出切分为 key 互不相交的部分。HFileOutputFormat2 类中的 configureIncrementalLoad() 方法会依据当前表
 * 中的 Region 边界自动设置 TotalOrderPartitioner。
 *
 *  2，完成数据导入
 *  一旦数据准备好，就可以使用 completebulkload 工具将生成的 HFile 导入HBase 集群中。completebulkload
 *  是一个命令行工具，对生成的 HFile 文件迭代进行处理，对每一个 HFile， 确定所属的 region， 然后联系对应
 *  的 RegionServer， 将数据移动至相应的存储路径。
 *  如果在准备数据过程中，或者在使用 completebulkload 导入数据过程中， region 的边界发生了改变（split），
 *  completebulkload 工具会按照新的边界自动切分数据文件。这个过程可能会对性能造成影响。
 *
 *  除了使用 completebulkload 工具外，也可以在程序中完成, LoadIncrementalHFiles 类提供了相应的方法。
 *
 *
 */


public class BulkLoadMR {

    public static void main(String[] args) throws Exception {

        //1，获取说需要的变量
        String config_path = args[0];

        String tableName = PropertiesUtils.get_NameValues(config_path,"tablename");
        String srcPath = PropertiesUtils.get_NameValues(config_path,"hdfs_src_path");
        String hFilePath = PropertiesUtils.get_NameValues(config_path,"hfile_outputdir");
        String zkAddress = PropertiesUtils.get_NameValues(config_path,"hbase_zookeeper");
        String hbase_master = PropertiesUtils.get_NameValues(config_path,"hbase_master");
        String hbase_site_path = PropertiesUtils.get_NameValues(config_path,"hbase_site_path");
        String columnField = PropertiesUtils.get_NameValues(config_path,"columnField");

        //2，创建hbase conf
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path(hbase_site_path));

        //3，创建hbase admin
        Configuration hadoopconf = new Configuration();
        Connection connection = ConnectionFactory.createConnection(hadoopconf);
        Admin admin = connection.getAdmin();

        //4，往map函数传递变量，主要有分隔符，rowkey，列
        conf.set("field",columnField);

        //5，生成hfile文件
        HTable table = new HTable(conf, tableName);
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

        //6，将hfile导入到hbase中
        LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(conf);
        loadFfiles.doBulkLoad(new Path(hFilePath), admin, table, table.getRegionLocator());

    }

}
