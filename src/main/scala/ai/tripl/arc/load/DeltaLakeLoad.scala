package ai.tripl.arc.load

import java.net.URI
import scala.collection.JavaConverters._
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import com.typesafe.config._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.ExtractUtils
import ai.tripl.arc.util.MetadataUtils
import ai.tripl.arc.util.ListenerUtils
import ai.tripl.arc.util.Utils

import io.delta.tables.DeltaTable
import org.apache.spark.sql.delta._
import org.apache.hadoop.fs.Path

class DeltaLakeLoad extends PipelineStagePlugin {

  val version = ai.tripl.arc.deltalake.BuildInfo.version

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "replaceWhere" :: "mergeSchema" :: "overwriteSchema" :: "outputMode" :: "checkpointLocation" :: "params" :: "options" :: "generateSymlinkManifest" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputURI = getValue[String]("outputURI") |> parseURI("outputURI") _
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _
    val outputMode = getValue[String]("outputMode", default = Some("Append"), validValues = "Append" :: "Complete" :: "Update" :: Nil) |> parseOutputModeType("outputMode") _
    val options = {
      val userOptions = readMap("options", c)
      userOptions ++ List(("overwriteSchema", userOptions.getOrElse("overwriteSchema", "true")))
    }
    val params = readMap("params", c)
    val generateSymlinkManifest = getValue[java.lang.Boolean]("generateSymlinkManifest", default = Some(true))
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, outputMode, generateSymlinkManifest, invalidKeys) match {
      case (Right(name), Right(description), Right(inputView), Right(outputURI), Right(numPartitions), Right(authentication), Right(saveMode), Right(partitionBy), Right(outputMode), Right(generateSymlinkManifest), Right(invalidKeys)) =>

        val stage = DeltaLakeLoadStage(
          plugin=this,
          name=name,
          description=description,
          inputView=inputView,
          outputURI=outputURI,
          partitionBy=partitionBy,
          numPartitions=numPartitions,
          authentication=authentication,
          saveMode=saveMode,
          options=options,
          outputMode=outputMode,
          generateSymlinkManifest=generateSymlinkManifest,
          params=params
        )

        numPartitions.foreach { numPartitions => stage.stageDetail.put("numPartitions", Integer.valueOf(numPartitions)) }
        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("outputURI", outputURI.toString)
        stage.stageDetail.put("partitionBy", partitionBy.asJava)
        stage.stageDetail.put("saveMode", saveMode.toString.toLowerCase)
        stage.stageDetail.put("options", options.asJava)
        stage.stageDetail.put("generateSymlinkManifest", generateSymlinkManifest)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, outputMode, generateSymlinkManifest, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}


case class DeltaLakeLoadStage(
    plugin: DeltaLakeLoad,
    name: String,
    description: Option[String],
    inputView: String,
    outputURI: URI,
    partitionBy: List[String],
    numPartitions: Option[Int],
    authentication: Option[Authentication],
    saveMode: SaveMode,
    outputMode: OutputModeType,
    params: Map[String, String],
    options: Map[String, String],
    generateSymlinkManifest: Boolean
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    DeltaLakeLoadStage.execute(this)
  }
}

object DeltaLakeLoadStage {

  def execute(stage: DeltaLakeLoadStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val df = spark.table(stage.inputView)

    if (!df.isStreaming) {
      stage.numPartitions match {
        case Some(partitions) => stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(partitions))
        case None => stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(df.rdd.getNumPartitions))
      }
    }

    // set write permissions
    CloudUtils.setHadoopConfiguration(stage.authentication)

    val dropMap = new java.util.HashMap[String, Object]()

    // Parquet cannot handle a column of NullType
    val nulls = df.schema.filter( _.dataType == NullType).map(_.name)
    if (!nulls.isEmpty) {
      dropMap.put("NullType", nulls.asJava)
    }

    stage.stageDetail.put("drop", dropMap)

    val nonNullDF = df.drop(nulls:_*)

    try {
      if (nonNullDF.isStreaming) {
        stage.partitionBy match {
          case Nil => nonNullDF.writeStream.format("delta").outputMode(stage.outputMode.sparkString).options(stage.options).start(stage.outputURI.toString)
          case partitionBy => {
            val partitionCols = partitionBy.map(col => nonNullDF(col))
            nonNullDF.writeStream.partitionBy(partitionBy:_*).format("delta").outputMode(stage.outputMode.sparkString).options(stage.options).start(stage.outputURI.toString)
          }
        }
      } else {
        stage.partitionBy match {
          case Nil => {
            stage.numPartitions match {
              case Some(n) => nonNullDF.repartition(n).write.format("delta").mode(stage.saveMode).options(stage.options).save(stage.outputURI.toString)
              case None => nonNullDF.write.format("delta").mode(stage.saveMode).options(stage.options).save(stage.outputURI.toString)
            }
          }
          case partitionBy => {
            // create a column array for repartitioning
            val partitionCols = partitionBy.map(col => nonNullDF(col))
            stage.numPartitions match {
              case Some(n) => nonNullDF.repartition(n, partitionCols:_*).write.format("delta").partitionBy(partitionBy:_*).mode(stage.saveMode).options(stage.options).save(stage.outputURI.toString)
              case None => nonNullDF.repartition(partitionCols:_*).write.format("delta").partitionBy(partitionBy:_*).mode(stage.saveMode).options(stage.options).save(stage.outputURI.toString)
            }
          }
        }

        // symlink generation to support presto reading the output
        if (stage.generateSymlinkManifest) {
          val deltaTable = DeltaTable.forPath(stage.outputURI.toString)
          deltaTable.generate("symlink_format_manifest")
        }

        // version logging
        val deltaLog = DeltaLog.forTable(spark, new Path(stage.outputURI.toString))
        val commitInfos = deltaLog.history.getHistory(Some(1))
        val commitInfo = commitInfos(0)
        val commitMap = new java.util.HashMap[String, Object]()
        commitMap.put("version", java.lang.Long.valueOf(commitInfo.getVersion))
        commitMap.put("timestamp", Instant.ofEpochMilli(commitInfo.getTimestamp).atZone(ZoneId.systemDefault).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
        stage.stageDetail.put("commit", commitMap)

      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    Option(nonNullDF)
  }
}