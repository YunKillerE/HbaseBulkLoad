package nova.untils;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by yunchen on 2017/3/28.
 */
public class BulkLoadUntils {
    /**
     * 1，分隔符函数
     * 2，rowkey函数
     * 3，列函数
     */

    /**
     * 这个函数基本上不会变化，就传入一个分隔符就行
     * @param line  行数据
     * @param filed 分隔符
     * @return
     */
    public static String[] fieldData(String line,String filed){
        String[] splited = line.split(filed);
        return splited;
    }

    /**
     *这里涉及到rowkey的设计，根据不同的组合进行判断
     * @param splited   行数据转化为String数组
     * @return
     */
    public static byte[] rowkeyBytes(String[] splited){
        byte[] rowkeybytes = Bytes.toBytes(splited[1].substring(1, splited[1].length()) + "-" + CommonUntils.getRandomString(6));
        return rowkeybytes;
    }

    /**
     * 这里比较复杂，很难覆盖所有场景，有些场景会比较特殊，比如dmp项目的根据列值的前几位来确定列名，
     * 绍兴公安项目电动车轨迹是需要将多个列的值组合成json存到一个列中，这些情况需要单独写相应的逻辑来处理
     * @param rowkeybytes
     * @param splited
     * @return
     */
    public static Put columnPut(byte[] rowkeybytes,String[] splited){
        Put put = new Put(rowkeybytes);
        put.addColumn("info".getBytes(), "time".getBytes(), splited[0].getBytes());
        put.addColumn("info".getBytes(), "content".getBytes(), splited[2].getBytes());
        put.addColumn("info".getBytes(), "frequency".getBytes(), splited[3].getBytes());
        put.addColumn("info".getBytes(), "comment".getBytes(), splited[4].getBytes());
        return put;
    }


}
