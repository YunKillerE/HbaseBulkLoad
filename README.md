# HbaseBulkLoad
基于BulkLoad的通用的hbase导入程序


   [配置文件介绍](https://github.com/jimmy-src/HbaseBulkLoad/wiki/%E9%85%8D%E7%BD%AE%E6%96%87%E4%BB%B6)


原理：

1，通过 MapReduce 任务生成 HFile

    在进行数据导入时，需要对数据进行预处理，如过滤无效数据、数据格式转换等。通常按照不同的导入要求，
    需要编写不同的 Mapper；Reducer 由 HBase 负责处理。为了按照 HBase 内部存储格式生成数据，一个重要的类是
    HFileOutputFormat2(HBase 1.0.0以前版本使用 HFileOutputFormat)。为了更有效地导入数据，
    每一个输出的 HFile 要恰好适应一个 Region。为了确保这一点， 需要使用 TotalOrderPartitioner 类将 map
    的输出切分为 key 互不相交的部分。HFileOutputFormat2 类中的 configureIncrementalLoad() 方法会依据当前表
    中的 Region 边界自动设置 TotalOrderPartitioner。

2，完成数据导入

    一旦数据准备好，就可以使用 completebulkload 工具将生成的 HFile 导入HBase 集群中。completebulkload
    是一个命令行工具，对生成的 HFile 文件迭代进行处理，对每一个 HFile， 确定所属的 region， 然后联系对应
    的 RegionServer， 将数据移动至相应的存储路径。
    如果在准备数据过程中，或者在使用 completebulkload 导入数据过程中， region 的边界发生了改变（split），
    completebulkload 工具会按照新的边界自动切分数据文件。这个过程可能会对性能造成影响。
    除了使用 completebulkload 工具外，也可以在程序中完成, LoadIncrementalHFiles 类提供了相应的方法。


 思路：
 
 1，rowkey的设计
 
      1）列中的某一个字段作为rwokey
      2）列中的某一个字段加上随机数作为rowkey
      3）多个列组合加随机数
      4）某个列的一部分加随机数
      
 2，列
 
      1）每一列作为一个列
      2）多个列组合作为一个列
      
 3，列族
 
      1）通过一个数组或者字符串获取所有数组
      2）需要事先定义好哪些列属于哪个列族


 工作计划：
 
     1，列族组合功能
     2，表创建功能，主要通过列族来创建表，这里涉及到rowkey散列和预分区问题，和rowkey的设计会相关
     3，rowkey拼接功能，根据前面列举的几中情况来写相应的函数
     4，列的组合功能，这里相对比较复杂，可能需要根据场景来


以后还会加入更多的rowkey设计方案，以及预分区方案，对于列这一块，目前实现的是给定列族和列名称的情况，
如果列名称不确定，则需要自己系这一块的逻辑

目前遇到一个问题没有解决：

     在将mapreduce改为spark时，map函数不知道怎么写，试了很多种方法都不行，希望大神们不吝赐教！！
