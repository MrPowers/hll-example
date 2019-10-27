package mrpowers.hll.example

import org.scalatest.FunSpec

import com.swoop.alchemy.spark.expressions.hll.functions._

class DoEverythingSpec extends FunSpec with SparkSessionTestWrapper {

  it("does everything") {

    val path1 = new java.io.File("./src/test/resources/users1.csv").getCanonicalPath

    val df1 = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(path1)

    val res1 = df1
      .groupBy("user_id")
      .agg(hll_init_agg("user_id").as("user_id_hll"))

    val sketchPath1 = new java.io.File("./tmp/sketches/file1").getCanonicalPath

    res1.write.parquet(sketchPath1)

    val path2 = new java.io.File("./src/test/resources/users2.csv").getCanonicalPath

    val df2 = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(path2)

    val res2 = df2
      .groupBy("user_id")
      .agg(hll_init_agg("user_id").as("user_id_hll"))

//    val firstAggregation =

  }

}
