package nova.untils;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.util.Arrays;
import java.util.List;

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
     *
     *  * 1，rowkey的设计
     *      1）列中的某一个字段作为rwokey
     *      2）列中的某一个字段加上随机数作为rowkey
     *      3）多个列组合加随机数
     *      4）某个列的一部分加随机数
     *
     * @param splited   行数据转化为String数组
     * @return
     */
    public static byte[] rowkeyBytes(String[] splited, String splitType, String HowNumLine, String rowkeyLine, String issubstring,
                                     Integer substringbegin, Integer substringend){

        //用来判断几个列作为rowkey
        String[] numLineList = rowkeyLine.split(",");

        /**
         * 1，判断有几个列用来作为rowkey组合，并获取列的值
         */
        String rowkeyline = null;

        //当为单列时rowkey组合情况
        if(numLineList.length == 1) {
            rowkeyline = splited[Integer.parseInt(numLineList[0])];
            //当为多列是rowkey组合情况
        }else{
            String rowkeylinetmp = "";
            if(issubstring.equals("true")){
                for(int i=0;i<numLineList.length;i++){
                    if(rowkeyline.length() == 0) {
                        rowkeylinetmp = splited[Integer.parseInt(numLineList[i])].substring(substringbegin,substringend);
                    }else{
                        rowkeylinetmp = rowkeylinetmp+splited[Integer.parseInt(numLineList[i])].substring(substringbegin,substringend);
                    }
                }
            }
            rowkeyline = rowkeylinetmp;
        }

       // System.out.println("当前处理的rowkey是=============================="+rowkeyline);
        CommonUntils.log.info("当前处理的rowkey是=============================="+rowkeyline);

        byte[] rowkeybytes = new byte[0];

        /**
         * 2，判断预分区方式，hash,partition,none，然后确定对应的rowkey组合方式
         */
            //这里是通过hash方式进行rowkey设计
        if(splitType.equals("hash")) {
            //列截取列中的几个字符情况
            if (issubstring.equals("true") && numLineList.length == 1) {
                rowkeybytes = Bytes.add(MD5Hash.getMD5AsHex(Bytes.toBytes(rowkeyline)).substring(substringbegin,
                        substringend).getBytes(), Bytes.toBytes(rowkeyline));
                //列不截取字符的情况
            } else if (issubstring.equals("false") && numLineList.length == 1) {
                rowkeybytes = Bytes.add(MD5Hash.getMD5AsHex(Bytes.toBytes(rowkeyline)).getBytes(),
                        Bytes.toBytes(rowkeyline));
            }else {
                System.out.println(issubstring + "input error!! only true or false");
                System.exit(1);
            }
            //这里通过partition方式分区时的rowkey设计
        }else if(splitType.equals("partition")){
            System.out.println("功能待加入");
            //这直接用某一个或者多个列作为rowkey
        }else if(splitType.equals("none")){
            rowkeybytes = Bytes.toBytes(rowkeyline);
        }else{
            System.out.println("预分区方式名称输入错误，仅支持hash/partition/none三种方式");
            System.exit(1);
        }

        /**
         * TODO 这里判断rowkey是否有问题，比如是否为空，是否异常等等,这里可以考虑跳过还是自己退出
         *
         * 这里还是跳过吧，在下一步做一些判断，如果为空则什么都不做
         */
        if(rowkeybytes.length==0){
            System.out.println(splited+"rowkey获取的值为空，请检查："+rowkeyline+"这一列的值是否有问题");
            //System.exit(1);
        }

        return rowkeybytes;

    }

    /**
     * 这里比较复杂，很难覆盖所有场景，有些场景会比较特殊，比如dmp项目的根据列值的前几位来确定列名，
     * 绍兴公安项目电动车轨迹是需要将多个列的值组合成json存到一个列中，这些情况需要单独写相应的逻辑来处理
     * @param rowkeybytes
     * @param splited
     * @return
     */
    public static Put columnPut(byte[] rowkeybytes,String[] splited,String columnFamily,String columnsName,String columnsName_qua){
        Put put = new Put(rowkeybytes);
        /*put.addColumn("info".getBytes(), "time".getBytes(), splited[0].getBytes());
        put.addColumn("info".getBytes(), "content".getBytes(), splited[2].getBytes());
        put.addColumn("info".getBytes(), "frequency".getBytes(), splited[3].getBytes());
        put.addColumn("info".getBytes(), "comment".getBytes(), splited[4].getBytes());*/

        //获取相应数组
        List<String> columnFamilyList = Arrays.asList(columnFamily.split(","));
        List<String> columnsNameList = Arrays.asList(columnsName.split(","));
        List<String> columnsName_quaList = Arrays.asList(columnsName_qua.split(","));

        //判断columnFamilyArray列族数组所有的值在columnsNameArray数组中都有
        if(columnsNameList.containsAll(columnFamilyList)){
            System.out.println("列族和列配置正确");
        }else{
            System.out.println("列族和列配置不正确，需要查看所有列族是否都在columnsName中");
        }


       /* columnsName_qua=1,3,2,4
        columnsName=info,time,content,comment,f1,frequency*/

        //循环列族，在内嵌循环列
        int quatmp = 0;//目的是获取列的值，定位到哪一列
        for (String cols:columnFamilyList) {
            int i = columnsNameList.indexOf(cols);

            for (int j = 1; j < columnsNameList.size(); j++) {
                if ( i+j >= columnsNameList.size() || columnFamily.contains(columnsNameList.get(i + j))) {
                    break ;
                }
                put.addColumn(cols.getBytes(), columnsNameList.get(i+j).getBytes(), splited[Integer.parseInt(columnsName_quaList.get(quatmp))].getBytes());
                quatmp++;
            }

        }


        return put;
    }


}
