package nova.untils;

import nova.RowKey.HashChoreWoker;
import nova.RowKey.PartitionRowKeyManager;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by yunchen on 2017/3/29.
 */
public class HBaseUntils {

    private static Admin admin;
    private static Connection connection;

    public HBaseUntils(Admin a, Connection c){
        admin = a;
        connection = c;
    }

    /**
     * rowkey随机散列方式预分区
     *
     * 分区方式：
     *  1，hash就是rowkey前面由一串随机字符串组成,随机字符串生成方式可以由SHA或者MD5等方式生成，只要region所管理的start-end keys范围比较随机，
     *      那么就可以解决写热点问题。
     * long currentId = 1L;
     * byte [] rowkey = Bytes.add(MD5Hash.getMD5AsHex(Bytes.toBytes(currentId)).substring(0, 8).getBytes(),Bytes.toBytes(currentId));
     *
     *  2，partition故名思义，就是分区式，这种分区有点类似于mapreduce中的partitioner,将区域用长整数(Long)作为分区号，
     *      每个region管理着相应的区域数据，在rowKey生成时，将id取模后，然后拼上id整体作为rowKey.这个比较简单，不需要取样，splitKeys也非常简单，直接是分区号即可。
     *
     * @param tableNmae 表名
     * @param cols  列族数组
     * @param compressType  压缩格式
     * @param splitType 分区方式
     * @param regionSplitNum 压缩方式
     * @throws IOException
     *
     */
    public static void createTableBySplitKeys(String tableNmae,String[] cols,String compressType,
                                              Integer regionSplitNum,String splitType) throws IOException {
        byte[][] splitKeys = new byte[0][];

        if(splitType.equals("hash")) {
            HashChoreWoker worker = new HashChoreWoker(1000000, regionSplitNum);
            splitKeys = worker.calcSplitKeys();
        }else if(splitType.equals("partition")){
            PartitionRowKeyManager rkManager = new PartitionRowKeyManager();
            //只预建10个分区
            rkManager.setPartition(10);
            splitKeys = rkManager.calcSplitKeys();
        }else if(splitKeys.equals("none")){
            System.out.println("不进行预分区");
        }

        TableName tableName = TableName.valueOf(tableNmae);

        Compression.Algorithm compress = null;
        if(compressType.equals("NONE")) {
            compress = Compression.Algorithm.NONE;
        }else if(compressType.equals("SNAPPY")) {
            compress = Compression.Algorithm.SNAPPY;
        }else if(compressType.equals("GZ")){
            compress = Compression.Algorithm.GZ;
        }else if(compressType.equals("LZ4")){
            compress = Compression.Algorithm.LZ4;
        }else if(compressType.equals("LZO")){
            compress = Compression.Algorithm.LZO;
        }else{
            System.out.println(compressType+"：是不支持的压缩方式，或者拼写错误，仅支持SNAPPY/GZ/LZ4/LZO(需要集群支持)/NONE");
            System.exit(1);
        }

        if(admin.tableExists(tableName)){
            System.out.println("talbe is exists!");
        }else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for(String col:cols){
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hColumnDescriptor.setMaxVersions(3);
                hColumnDescriptor.setCompressionType(compress);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            if(splitKeys.equals("none")) {
                admin.createTable(hTableDescriptor);
            }else{
                admin.createTable(hTableDescriptor, splitKeys);
            }
        }
    }


    //关闭连接
    public static  void close(){
        try {
            if(null != admin)
                admin.close();
            if(null != connection)
                connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //建表
    public static void createTable(String tableNmae,String[] cols) throws IOException {

        TableName tableName = TableName.valueOf(tableNmae);

        if(admin.tableExists(tableName)){
            System.out.println("talbe is exists!");
        }else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for(String col:cols){
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }

    }

    //删表
    public static void deleteTable(String tableName) throws IOException {

        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
        close();
    }

    //查看已有表
    public static void listTables() throws IOException {

        HTableDescriptor hTableDescriptors[] = admin.listTables();
        for(HTableDescriptor hTableDescriptor :hTableDescriptors){
            System.out.println(hTableDescriptor.getNameAsString());
        }
        close();
    }

    //插入数据
    public static void insterRow(String tableName,String rowkey,String colFamily,String col,String val) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        table.put(put);

        //批量插入
       /* List<Put> putList = new ArrayList<Put>();
        puts.add(put);
        table.put(putList);*/
        table.close();
        close();
    }

    //删除数据
    public static void deleRow(String tableName,String rowkey,String colFamily,String col) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        //删除指定列族
        //delete.addFamily(Bytes.toBytes(colFamily));
        //删除指定列
        //delete.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        table.delete(delete);
        //批量删除
       /* List<Delete> deleteList = new ArrayList<Delete>();
        deleteList.add(delete);
        table.delete(deleteList);*/
        table.close();
        close();
    }

    //根据rowkey查找数据
    public static void getData(String tableName,String rowkey,String colFamily,String col)throws  IOException{

        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        //获取指定列族数据
        //get.addFamily(Bytes.toBytes(colFamily));
        //获取指定列数据
        //get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        Result result = table.get(get);

        showCell(result);
        table.close();
        close();
    }

    //格式化输出
    public static void showCell(Result result){
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){
            System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
            System.out.println("Timetamp:"+cell.getTimestamp()+" ");
            System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
            System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
            System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
        }
    }

    //批量查找数据
    public static void scanData(String tableName,String startRow,String stopRow)throws IOException{

        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        //scan.setStartRow(Bytes.toBytes(startRow));
        //scan.setStopRow(Bytes.toBytes(stopRow));
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result : resultScanner){
            showCell(result);
        }
        table.close();
        close();
    }

}
