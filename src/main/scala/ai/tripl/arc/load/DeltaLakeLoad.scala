package ai.tripl.arc.load

import java.net.URI
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

import scala.collection.JavaConverters._
import scala.util.Try

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

class DeltaLakeLoad extends PipelineStagePlugin with JupyterCompleter {

  val version = ai.tripl.arc.deltalake.BuildInfo.version

  def snippet()(implicit arcContext: ARCContext): String = {
    s"""{
    |  "type": "DeltaLakeLoad",
    |  "name": "DeltaLakeLoad",
    |  "environments": [${arcContext.completionEnvironments.map { env => s""""${env}""""}.mkString(", ")}],
    |  "inputView": "inputView",
    |  "outputURI": "hdfs://*.delta"
    |}""".stripMargin
  }

  val documentationURI = new java.net.URI(s"${baseURI}/load/#deltalakeload")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "id" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "replaceWhere" :: "mergeSchema" :: "overwriteSchema" :: "outputMode" :: "checkpointLocation" :: "params" :: "options" :: "generateSymlinkManifest" :: Nil
    val id = getOptionalValue[String]("id")
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

    (id, name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, outputMode, generateSymlinkManifest, invalidKeys) match {
      case (Right(id), Right(name), Right(description), Right(inputView), Right(outputURI), Right(numPartitions), Right(authentication), Right(saveMode), Right(partitionBy), Right(outputMode), Right(generateSymlinkManifest), Right(invalidKeys)) =>

        val stage = DeltaLakeLoadStage(
          plugin=this,
          id=id,
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
        val allErrors: Errors = List(id, name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, outputMode, generateSymlinkManifest, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}


case class DeltaLakeLoadStage(
    plugin: DeltaLakeLoad,
    id: Option[String],
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
  ) extends LoadPipelineStage {

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

    // DeltaLakeLoad cannot handle a column of NullType
    val nulls = df.schema.filter( _.dataType == NullType).map(_.name)
    val nonNullDF = if (!nulls.isEmpty) {
      val dropMap = new java.util.HashMap[String, Object]()
      dropMap.put("NullType", nulls.asJava)
      if (arcContext.dropUnsupported) {
        stage.stageDetail.put("drop", dropMap)
        df.drop(nulls:_*)
      } else {
        throw new Exception(s"""inputView '${stage.inputView}' contains types ${new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(dropMap)} which are unsupported by DeltaLakeLoad and 'dropUnsupported' is set to false.""")
      }
    } else {
      df
    }

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
        commitInfo.operationMetrics.foreach { operationMetrics => commitMap.put("operationMetrics", operationMetrics.map { case (k, v) => (k, Try(v.toInt).getOrElse(v)) }.asJava) }
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