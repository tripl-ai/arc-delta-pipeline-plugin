package ai.tripl.arc

import java.net.URI
import java.util.UUID
import java.io.File

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.avro.Schema
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.util._

import io.delta.tables._

class DeltaLakeMergeLoadSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val inputView = "dataset"

  val addPattern = "add.*(part-[^\"]*)".r
  val removePattern = "remove.*(part-[^\"]*)".r

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .config("spark.databricks.delta.merge.optimizeInsertOnlyMerge.enabled", false)
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("DEBUG")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
    import spark.implicits._
  }

  after {
    session.stop()
  }

  def getListOfFiles(dir: String):List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
          d.listFiles.filter(_.isFile).toList
      } else {
          List[File]()
      }
  }

  test("DeltaLakeMergeLoad: end-to-end") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val testUUID = UUID.randomUUID.toString
    val output = s"${FileUtils.getTempDirectoryPath}/${testUUID}"

    // create overlapping datasets with insert, update and delete cases
    Seq((0, "a"),(1, "b"),(2, "c")).toDF("key", "value").write.format("delta").mode("overwrite").save(output)
    Seq((1, "b"),(2, "c"),(3, "d")).toDF("key", "value").createOrReplaceTempView(inputView)

    val conf = s"""{
      "stages": [
        {
          "type": "DeltaLakeMergeLoad",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "outputURI": "${output}",
          "inputView": "${inputView}",
          "condition": "source.key = target.key",
          "whenNotMatchedInsert": {}
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline)(spark, logger, arcContext).get

        val df0 = spark.read.format("delta").option("versionAsOf", 0).load(output).withColumn("_filename", input_file_name()).orderBy(col("key"))
        val df1 = spark.read.format("delta").option("versionAsOf", 1).load(output).withColumn("_filename", input_file_name()).orderBy(col("key"))

        df0.joinWith(df1, df0("_filename") === df1("_filename"), "full").show(false)
        // val changes = spark.read.option("wholetext", "true").text(s"${output}/_delta_log/00000000000000000001.json").collect.mkString("\n")
        // val addFiles = addPattern.findAllMatchIn(changes).map(m => m.group(1)).toSet
        // val removeFiles = removePattern.findAllMatchIn(changes).map(m => m.group(1)).toSet

        // println(addFiles)
        // println(removeFiles)        
      }
    }
  }

//   test("DeltaLakeMergeLoad: converge") {
//     implicit val spark = session
//     import spark.implicits._
//     implicit val logger = TestUtils.getLogger()
//     implicit val arcContext = TestUtils.getARCContext(isStreaming=false)


//     val addPattern = "add.*(part-[^\"]*)".r
//     val removePattern = "remove.*(part-[^\"]*)".r

//     val testUUID = UUID.randomUUID.toString
//     val output = s"${FileUtils.getTempDirectoryPath}/${testUUID}"

//     val df0 = spark.sqlContext.range(0, 10).repartition(10, col("id"))

//     df0.write.format("delta").mode("overwrite").save(output)


//     for (i <- 1 to 2) {

//       // println(s"\n00000000000000000000${(i-1).toString}.json")
//       // spark.read.text(s"${output}/_delta_log/0000000000000000000${(i-1).toString}.json").collect.foreach { row =>
//       //   val value = row.getString(0)
//       //   println(value)
//       // }

//       val changes = spark.read.option("wholetext", "true").text(s"${output}/_delta_log/0000000000000000000${(i-1).toString}.json").collect.mkString("\n")
//       val addFiles = addPattern.findAllMatchIn(changes).map(m => m.group(1)).toSet
//       val removeFiles = removePattern.findAllMatchIn(changes).map(m => m.group(1)).toSet

//       println(addFiles)
//       println(removeFiles)

//       val df1 = spark.sqlContext.range(i, i+10).repartition(10, col("id"))

//       val deltaTable = DeltaTable.forPath(spark, output)

//       deltaTable
//         .as("target")
//         .converge(
//           df1.as("source"),
//           "source.id = target.id"
//         )
        
//       // println(s"\n0000000000000000000${i.toString}.json")
//       // spark.read.text(s"${output}/_delta_log/0000000000000000000${i.toString}.json").collect.foreach { row =>
//       //   val value = row.getString(0)
//       //   println(value)
//       // }

//       // spark.read.format("delta").option("versionAsOf", i-1).load(output).withColumn("input_file_name", input_file_name).show(false)
//       // spark.read.format("delta").option("versionAsOf", i).load(output).withColumn("input_file_name", input_file_name).show(false)
//     }
//   }  

}