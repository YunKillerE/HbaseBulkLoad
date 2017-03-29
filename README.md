# HbaseBulkLoad
基于BulkLoad的通用的hbase导入程序


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