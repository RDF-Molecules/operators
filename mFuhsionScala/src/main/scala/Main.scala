/**
  * Created by dcollarana on 11/30/2016.
  */
object Main {

  def main(args: Array[String]): Unit = {
    val t = new Tuple2[String, Int]("Hello",3)
    val t3 = new Tuple3[String, Int, Int]("Hello", 2, 4)
    println(t.toString())
    println(t3.toString())
  }

}
