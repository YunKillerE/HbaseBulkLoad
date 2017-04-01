package nova.untils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.TestMerge;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by yunchen on 2017/3/28.
 */

public class CommonUntils {

    public static final Log log = LogFactory.getLog(TestMerge.MyMapper.class);

    /**
     *
     * @param length length表示生成字符串的长度
     * @return
     */
    public static String getRandomString(int length) {
        String base = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(base.length());
            sb.append(base.charAt(number));
        }
        return sb.toString();
    }



    public static void test(){
        List<List> list = new ArrayList<>();

        List<String> aa = new ArrayList<>();
        List<String> bb = new ArrayList<>();

        aa.add("fam1");
        aa.add("qua1");
        aa.add("qua2");

        bb.add("fam2");
        bb.add("qua3");
        bb.add("qua4");

        list.add(aa);
        list.add(bb);

        for(List l:list){
            System.out.println(l);
            for(Object s:l){
                System.out.println(s);
            }
        }
    }


}
