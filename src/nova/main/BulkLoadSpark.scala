package nova.main

import java.io.IOException

import nova.untils.PropertiesUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yunchen on 2017/3/24.
  */
object BulkLoadSpark {

  def main(args: Array[String]): Unit = {

    val config_path = args(1)

    val hbase_zookeeper = PropertiesUtils.get_NameValues(config_path, "hbase_zookeeper")
    val hbase_master = PropertiesUtils.get_NameValues(config_path, "hbase_master")
    val tablename = PropertiesUtils.get_NameValues(config_path, "tablename")
    val hfile_outputdir = PropertiesUtils.get_NameValues(config_path, "hfile_outputdir")
    val hdfs_src_path = PropertiesUtils.get_NameValues(config_path, "hdfs_src_path")

    //创建hbaseconf
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", hbase_zookeeper)
    conf.set("hbase.master", hbase_master)
    conf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    //创建sc
    val sparkconf = new SparkConf().setAppName("BulkLoadSpark")
    val sc = new SparkContext(sparkconf)

    //创建hbase的admin
    val hadoopconf = new Configuration()
    val connection = ConnectionFactory.createConnection(hadoopconf)
    val admin = connection.getAdmin

    //HFile相关job输出配置
    val table = new HTable(conf, tablename)
    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoad(job, table.getTableDescriptor, table.getRegionLocator)

    //生成测试随机数，实际情况应该是从hdfs上读取数据
        val num = sc.parallelize(31 to 40)
    val rdd = num.map(x => {
      val kv: KeyValue = new KeyValue(Bytes.toBytes(x), "cf".getBytes(), "c1".getBytes(), "value_xxx".getBytes())
      (new ImmutableBytesWritable(Bytes.toBytes(x)), kv)
    })

    //val lines = sc.textFile(hdfs_src_path)

    //直接报错了，多个列的情况下无法实现，不知道错在哪里，而上面的随机数却可以
/*    val rdd = lines.map(x => {
      val splited: Array[String] = x.split(",")

      val p = new Put(Bytes.toBytes(splited(1)))
      p.addColumn("info".getBytes, "time".getBytes, splited(0).getBytes)
      p.addColumn("info".getBytes, "content".getBytes, splited(2).getBytes)
      p.addColumn("info".getBytes, "frequency".getBytes, splited(3).getBytes)
      p.addColumn("info".getBytes, "comment".getBytes, splited(4).getBytes)

      (new ImmutableBytesWritable, p)

    }).sortByKey(true)*/


    //save hfiles to hdfs
    //rdd.saveAsNewAPIHadoopFile("/yyy",classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat],conf)
    //用这个会报错，网上说是spark的版本和hadoop的版本api不匹配的问题
    job.getConfiguration.set("mapreduce.output.fileoutputformat.outputdir", hfile_outputdir)
    rdd.sortByKey(true).saveAsNewAPIHadoopDataset(job.getConfiguration)

    //bulk load hfiles to hbase
    /*val bulkLoader = new LoadIncrementalHFiles(conf)
    bulkLoader.doBulkLoad(new Path(hfile_outputdir), admin, table, table.getRegionLocator)*/
    //老api
    //bulkLoader.doBulkLoad(new Path("/yunchen"),table)

    //关闭连接
    //def close() = {
    try {
      if (null != admin)
        admin.close()
      if (null != connection)
        connection.close()
    } catch {
      case ex: IOException => {
        println("IO Exception")
      }
    }
  }

}