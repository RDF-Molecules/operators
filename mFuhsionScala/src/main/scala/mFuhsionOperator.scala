/**
  * Created by dcollarana on 11/30/2016.
  */
class mFuhsionOperator extends mFuhsionTrait {

  override def mFuhsion(rtl_1: RTL, rtl_2: RTL, similarity: Array[Array[Double]], threshold: Double, table_1: List[RTL], table_2: List[RTL]): List[(String, String)] = {

    val result_1 = stage(rtl_1, table_2, table_1, similarity, threshold)
    val result_2 = stage(rtl_2, table_1, table_2, similarity, threshold)
    return result_1.union(result_2)

  }

  override def stage(rtl: RTL, own_table: List[RTL], dif_table: List[RTL], similarity: Array[Array[Double]], threshold: Double): List[(String, String)] = {

    //insert
    insert(rtl, own_table)
    //probe
    return probe(rtl, dif_table, similarity, threshold)

  }

  override def insert(rtl: RTL, table: List[RTL]) = {
    table.::(rtl)
  }

  override def probe(rtl: RTL, table: List[RTL], similarity: Array[Array[Double]], threshold: Double): List[(String, String)] = {
    //val toBeJoined = new List[Tuple2[String, String]]()
    /*
     probing_head = rtl1.head
    for some_rtl in table:
        head = some_rtl.head

        # check similarity and threshold
        if sim(probing_head, head, similarity) > threshold:
            #produce join
            toBeJoined.append((rtl1, some_rtl))

    return toBeJoined
     */
    null
  }

  override def sim(rtl_1: RTL, rtl_2: RTL, similarity: Array[Array[Double]]): Double = {
    return similarity.apply(rtl_1.head._2).apply(rtl_2.head._2)
  }

}
