/**
  * Created by dcollarana on 11/30/2016.
  */
class mFuhsionOperator extends mFuhsionTrait {

  override def mFuhsion(rtl_1: RTL, rtl_2: RTL, similarity: Array[Array[Float]], threshold: Float, table_1: List[String], table_2: List[String]): List[(String, String)] = ???

  override def insert(rtl: RTL, table: List[String]): Unit = ???

  override def probe(rtl: RTL, table: List[String], similarity: Array[Array[Float]], threshold: Float): List[(String, String)] = ???

  override def stage(rtl: RTL, own_table: List[String], dif_table: List[String], similarity: Array[Array[Float]], threshold: Float): List[(String, String)] = ???

  override def sim(rtl_1: RTL, rtl_2: RTL, similarity: Array[Array[Float]]): Float = ???

}
