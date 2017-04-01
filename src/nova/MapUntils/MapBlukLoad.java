package nova.MapUntils;

import nova.untils.BulkLoadUntils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by yunchen on 2017/3/29.
 *
 *
 * 思路：
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
 */
public class MapBlukLoad {

    /**
     *参数设想：
     *  列族，列
     *  用List来表示：[{列族，列，列，列...},{列族，列，列，列...},{列族，列，列，列...}...]
     *
     */
    public static class Map extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String line = value.toString();

            /*//1，列的分割符
            String[] splited = line.split(",");
            //2，rowkey的设计
            byte[] rowkeybytes = Bytes.toBytes(splited[1].substring(1, splited[1].length()) + "-" + CommonUntils.getRandomString(6));
            ImmutableBytesWritable rowkey = new ImmutableBytesWritable(rowkeybytes);
            //3，列的设计
            Put put = new Put(rowkeybytes);
            put.addColumn("info".getBytes(), "time".getBytes(), splited[0].getBytes());
            put.addColumn("info".getBytes(), "content".getBytes(), splited[2].getBytes());
            put.addColumn("info".getBytes(), "frequency".getBytes(), splited[3].getBytes());
            put.addColumn("info".getBytes(), "comment".getBytes(), splited[4].getBytes());*/

            //1，分隔符处理调用untils中的分隔符函数
            String[] splited = BulkLoadUntils.fieldData(line,(context.getConfiguration().get("field")));

            //2，rowkey处理，调用untils中的rowkey函数
            String rowkeyLinetmp = context.getConfiguration().get("rowkeyLine");
            String issubstring = context.getConfiguration().get("issubstring");
            Integer substringbegin = Integer.valueOf(context.getConfiguration().get("substringbegin"));
            Integer substringend = Integer.valueOf(context.getConfiguration().get("substringend"));
            String HowNumLine = context.getConfiguration().get("HowNumLine");
            String splitType = context.getConfiguration().get("splitType");

            byte[] rowkeybytes = BulkLoadUntils.rowkeyBytes(splited,splitType,HowNumLine,rowkeyLinetmp,issubstring,substringbegin,substringend);

            //3，列的处理，调用untils中的column函数 TODO 待增加功能
            ImmutableBytesWritable rowkey = new ImmutableBytesWritable(rowkeybytes);
            Put put = BulkLoadUntils.columnPut(rowkeybytes,splited);

            context.write(rowkey, put);
        }
    }

}
