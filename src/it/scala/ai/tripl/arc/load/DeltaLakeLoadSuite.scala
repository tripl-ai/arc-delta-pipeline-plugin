package ai.tripl.arc

import java.net.URI
import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties
import java.util.UUID

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util._
import ai.tripl.arc.util.ControlUtils._

class DeltaLakeLoadSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val inputView = "inputView"

  val bucketName = "test"

  // minio seems to need ip address not hostname
  val minioHostPort = "http://localhost:9000"
  val minioAccessKey = "AKIAIOSFODNN7EXAMPLE"
  val minioSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

  val outputURI = s"s3a://${bucketName}/delta"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    implicit val logger = TestUtils.getLogger()

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")       

    session = spark
  }

  after {
  }

  test("DeltaLakeLoadSuite: batch") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val dataset = TestUtils.getKnownDataset
    dataset.createOrReplaceTempView(inputView)

    val conf = s"""{
      "stages": [
        {
          "type": "DeltaLakeLoad",
          "name": "try to load some data",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "${inputView}",
          "outputURI": "${outputURI}",
          "authentication": {
            "method": "AmazonAccessKey",
            "accessKeyID": "${minioAccessKey}",
            "secretAccessKey": "${minioSecretKey}",
            "endpoint": "${minioHostPort}"
          }          
        }
      ]
    }"""

    val pipelineEither = ConfigUtils.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(_) => {
        println(pipelineEither)  
        assert(false)
      }
      case Right((pipeline, _)) => {
        ARC.run(pipeline)
      }
    }

  }    

}
