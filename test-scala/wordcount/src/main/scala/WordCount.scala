import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount");
    val sc = new SparkContext(conf);
    sc.textFile(args(0)).flatMap(_.split(" ")).map((_ ,1)).reduceByKey(_ + _,1).sortBy(_._2,false).saveAsTextFile(args(1));
    sc.stop();
  }
}
