package nova.RowKey;

import java.util.Iterator;
import java.util.TreeSet;
import org.apache.hadoop.hbase.util.Bytes;

public class HashChoreWoker implements SplitKeysCalculator{
    //随机取机数目
    private int baseRecord;
    //rowkey生成器
    private RowKeyGenerator rkGen;
    //取样时，由取样数目及region数相除所得的数量.
    private int splitKeysBase;
    //splitkeys个数
    private int splitKeysNumber;
    //由抽样计算出来的splitkeys结果
    private byte[][] splitKeys;

    public HashChoreWoker(int baseRecord, int prepareRegions) {
        this.baseRecord = baseRecord;
        rkGen = new HashRowKeyGenerator();
        splitKeysNumber = prepareRegions - 1;

        splitKeysBase = baseRecord / prepareRegions;
    }

    public byte[][] calcSplitKeys() {
        splitKeys = new byte[splitKeysNumber][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

        for (int i = 0; i < baseRecord; i++) {
            rows.add(rkGen.nextId());
        }
        int pointer = 0;

        Iterator<byte[]> rowKeyIter = rows.iterator();

        int index = 0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            if ((pointer != 0) && (pointer % splitKeysBase == 0)) {
                if (index < splitKeysNumber) {
                    splitKeys[index] = tempRow;
                    index ++;
                }
            }
            pointer ++;
        }

        rows.clear();
        rows = null;
        return splitKeys;
    }

    public static void main(String[] args) {
        byte [][] temp = new HashChoreWoker(1000000, 38).calcSplitKeys();
        
        for(byte [] row : temp) {
            System.out.println(Bytes.toStringBinary(row));
        }
    }

}
