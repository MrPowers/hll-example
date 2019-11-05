package mrpowers.hll.example

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.github.mrpowers.spark.daria.utils.NioUtils
import org.scalatest.FunSpec
import com.swoop.alchemy.spark.expressions.hll.functions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class DoEverythingSpec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

  it("uses native Spark functions to run an approximate count") {

    val path1 = new java.io.File("./src/test/resources/users1.csv").getCanonicalPath

    val df1 = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(path1)

    val resDF = df1
      .agg(approx_count_distinct("user_id").as("approx_user_id_count"))

    val expectedDF = spark.createDF(
      List(
        (3L)
      ), List(
        ("approx_user_id_count", LongType, false)
      )
    )

    assertSmallDataFrameEquality(resDF, expectedDF)

  }

  it("incrementally updates a HLL sketch") {

    val tmpDir = new java.io.File("./tmp").getCanonicalPath
    NioUtils.removeAll(tmpDir)

    val path1 = new java.io.File("./src/test/resources/users1.csv").getCanonicalPath

    val df1 = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(path1)

//    df1
//      .withColumn("user_id_hll", hll_init("user_id"))
//      .show()
//
//    df1
//      .withColumn("user_id_hll", hll_init("user_id"))
//      .printSchema()
//
//    df1
//      .withColumn("user_id_hll", hll_init("user_id"))
//      .select(hll_merge("user_id_hll").as("user_id_hll"))
//      .show()

    val sketchPath1 = new java.io.File("./tmp/sketches/file1").getCanonicalPath

    df1
      .withColumn("user_id_hll", hll_init("user_id"))
      .select(hll_merge("user_id_hll").as("user_id_hll"))
      .write
      .parquet(sketchPath1)

    val sketch1 = spark.read.parquet(sketchPath1)

//    sketch1
//      .select(hll_cardinality("user_id_hll"))
//      .show()

    val path2 = new java.io.File("./src/test/resources/users2.csv").getCanonicalPath

    val df2 = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(path2)

    val sketchPath2 = new java.io.File("./tmp/sketches/file2").getCanonicalPath

    val resDF = df2
      .withColumn("user_id_hll", hll_init("user_id"))
      .select("user_id_hll")
      .union(sketch1)
      .select(hll_merge("user_id_hll").as("user_id_hll"))
      .select(hll_cardinality("user_id_hll"))

    val expectedDF = spark.createDF(
      List(
        (5L)
      ), List(
        ("hll_cardinality(user_id_hll)", LongType, true)
      )
    )

    assertSmallDataFrameEquality(resDF, expectedDF)

  }

  it("builds cohorts easily") {

    val path = new java.io.File("./src/test/resources/gamers.csv").getCanonicalPath

    val df = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(path)

    val adults = df
      .where(col("age") >= 18)
      .withColumn("user_id_hll", hll_init("user_id"))
      .select(hll_merge("user_id_hll").as("user_id_hll"))

    val adultsDF = adults
      .select(hll_cardinality("user_id_hll"))

    val expectedAdultsDF = spark.createDF(
      List(
        (5L)
      ), List(
        ("hll_cardinality(user_id_hll)", LongType, true)
      )
    )

    assertSmallDataFrameEquality(adultsDF, expectedAdultsDF)

    val favoriteGameSmash = df
      .where(col("favorite_game") === "smash")
      .withColumn("user_id_hll", hll_init("user_id"))
      .select(hll_merge("user_id_hll").as("user_id_hll"))

    val favoriteGameSmashDF = favoriteGameSmash
      .select(hll_cardinality("user_id_hll"))

    val expectedFavoriteGameSmashDF = spark.createDF(
      List(
        (3L)
      ), List(
        ("hll_cardinality(user_id_hll)", LongType, true)
      )
    )

    assertSmallDataFrameEquality(favoriteGameSmashDF, expectedFavoriteGameSmashDF)

    val resDF = adults
      .union(favoriteGameSmash)
      .select(hll_merge("user_id_hll").as("user_id_hll"))
      .select(hll_cardinality("user_id_hll"))

    val expectedDF = spark.createDF(
      List(
        (6L)
      ), List(
        ("hll_cardinality(user_id_hll)", LongType, true)
      )
    )

    assertSmallDataFrameEquality(resDF, expectedDF)

  }

  it("builds cohorts with groupBy") {

    val path = new java.io.File("./src/test/resources/gamers.csv").getCanonicalPath

    val df = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(path)
      .withColumn("is_adult", col("age") >= 18)

    val resDF = df
      .groupBy("is_adult", "favorite_game")
      .agg(hll_init_agg("user_id").as("user_id_hll"))

    // number of adults that like smash
    val count1 = resDF
      .where(col("is_adult") && col("favorite_game") === "smash")
      .select(hll_cardinality("user_id_hll"))

    val expectedCount1 = spark.createDF(
      List(
        (2L)
      ), List(
        ("hll_cardinality(user_id_hll)", LongType, true)
      )
    )

    // number of children that like smash
    val count2 = resDF
      .where(!col("is_adult") && col("favorite_game") === "smash")
      .select(hll_cardinality("user_id_hll"))

    val expectedCount2 = spark.createDF(
      List(
        (1L)
      ), List(
        ("hll_cardinality(user_id_hll)", LongType, true)
      )
    )

  }

}
