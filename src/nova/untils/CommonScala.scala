package nova.untils

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by yunchen on 2017/3/28.
  */
object CommonScala {

  def convert(triple: (String, String, String, String, String)): (ImmutableBytesWritable, Put) = {
    val p = new Put(Bytes.toBytes(triple._2))
    p.add(Bytes.toBytes("info"),
      Bytes.toBytes("time"),
      Bytes.toBytes(triple._1))
    p.add(Bytes.toBytes("info"),
      Bytes.toBytes("content"),
      Bytes.toBytes(triple._3))
    p.add(Bytes.toBytes("info"),
      Bytes.toBytes("frequency"),
      Bytes.toBytes(triple._4))
    p.add(Bytes.toBytes("info"),
      Bytes.toBytes("comment"),
      Bytes.toBytes(triple._5))
    return (new ImmutableBytesWritable, p)
  }

}
