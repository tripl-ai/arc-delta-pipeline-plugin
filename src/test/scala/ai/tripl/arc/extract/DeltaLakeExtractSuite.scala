package ai.tripl.arc

import java.net.URI
import java.util.UUID

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

class DeltaLakeExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val outputView = "dataset"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
    import spark.implicits._
  }

  after {
    session.stop()
  }

  test("DeltaLakeExtract: read relativeVersion") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val testUUID = UUID.randomUUID.toString
    val output = s"${FileUtils.getTempDirectoryPath}/${testUUID}"

    Seq((2)).toDF("version").write.format("delta").mode("overwrite").save(output)
    Seq((1)).toDF("version").write.format("delta").mode("overwrite").save(output)
    Seq((0)).toDF("version").write.format("delta").mode("overwrite").save(output)

    val conf = s"""{
      "stages": [
        {
          "type": "DeltaLakeExtract",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${output}",
          "outputView": "${outputView}",
          "options": {
            "relativeVersion": -2
          }
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline)(spark, logger, arcContext).get
        assert(df.first.getInt(0) == 2)
      }
    }
  }

  test("DeltaLakeExtract: read versionAsOf") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val testUUID = UUID.randomUUID.toString
    val output = s"${FileUtils.getTempDirectoryPath}/${testUUID}"

    Seq((2)).toDF("version").write.format("delta").mode("overwrite").save(output)
    Seq((1)).toDF("version").write.format("delta").mode("overwrite").save(output)
    Seq((0)).toDF("version").write.format("delta").mode("overwrite").save(output)

    val conf = s"""{
      "stages": [
        {
          "type": "DeltaLakeExtract",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${output}",
          "outputView": "${outputView}",
          "options": {
            "versionAsOf": 1
          }
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline)(spark, logger, arcContext).get
        assert(df.first.getInt(0) == 1)
      }
    }
  }

  test("DeltaLakeExtract: batch relative version") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val testUUID = UUID.randomUUID.toString
    val output = s"${FileUtils.getTempDirectoryPath}/${testUUID}"

    Seq((2)).toDF("version").write.format("delta").mode("overwrite").save(output)
    Seq((1)).toDF("version").write.format("delta").mode("overwrite").save(output)
    Seq((0)).toDF("version").write.format("delta").mode("overwrite").save(output)

    val thrown0 = intercept[Exception with DetailException] {
      val dataset = extract.DeltaLakeExtractStage.execute(
        extract.DeltaLakeExtractStage(
          plugin=new extract.DeltaLakeExtract,
          id=None,
          name=outputView,
          description=None,
          input=output,
          outputView=outputView,
          authentication=None,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          timeTravel=Some(extract.TimeTravel(Some(-3), None, None, None))
        )
      ).get
    }
    assert(thrown0.getMessage.contains("Cannot time travel Delta table to version -3. Available versions: [-2 ... 0]."))

    val dataset0 = extract.DeltaLakeExtractStage.execute(
      extract.DeltaLakeExtractStage(
        plugin=new extract.DeltaLakeExtract,
        id=None,
        name=outputView,
        description=None,
        input=output,
        outputView=outputView,
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        timeTravel=Some(extract.TimeTravel(Some(-3), None, Some(true), None))
      )
    ).get
    assert(dataset0.first.getInt(0) == 2)

    val dataset1 = extract.DeltaLakeExtractStage.execute(
      extract.DeltaLakeExtractStage(
        plugin=new extract.DeltaLakeExtract,
        id=None,
        name=outputView,
        description=None,
        input=output,
        outputView=outputView,
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        timeTravel=Some(extract.TimeTravel(Some(-2), None, None, None))
      )
    ).get

    assert(dataset1.first.getInt(0) == 2)
  }

  test("DeltaLakeExtract: bad option key") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val testUUID = UUID.randomUUID.toString
    val output = s"${FileUtils.getTempDirectoryPath}/${testUUID}"

    Seq((2)).toDF("version").write.format("delta").mode("overwrite").save(output)
    Seq((1)).toDF("version").write.format("delta").mode("overwrite").save(output)
    Seq((0)).toDF("version").write.format("delta").mode("overwrite").save(output)


    // deliberate typo relativeVersion -> rAlativeVersion
    val conf = s"""{
      "stages": [
        {
          "type": "DeltaLakeExtract",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${output}",
          "outputView": "${outputView}",
          "options": {
            "rAlativeVersion": -2
          }
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => assert(err.toString.contains("Invalid attribute 'rAlativeVersion'. Perhaps you meant one of: ['relativeVersion']."))
      case Right((_, _)) => {
        assert(false)
      }
    }
  }
}
