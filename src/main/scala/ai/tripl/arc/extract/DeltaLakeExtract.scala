package ai.tripl.arc.extract

import java.io._
import java.net.URI
import java.util.Properties
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Try

import com.typesafe.config._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.TaskContext

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
import ai.tripl.arc.util.Utils

import io.delta.tables._
import org.apache.spark.sql.delta._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.Literal

class DeltaLakeExtract extends PipelineStagePlugin with JupyterCompleter {

  val version = ai.tripl.arc.deltalake.BuildInfo.version

  def snippet()(implicit arcContext: ARCContext): String = {
    s"""{
    |  "type": "DeltaLakeExtract",
    |  "name": "DeltaLakeExtract",
    |  "environments": [${arcContext.completionEnvironments.map { env => s""""${env}""""}.mkString(", ")}],
    |  "inputURI": "hdfs://*.delta",
    |  "outputView": "outputView"
    |}""".stripMargin
  }

  val documentationURI = new java.net.URI(s"${baseURI}/extract/#deltalakeextract")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "id" :: "name" :: "description" :: "environments" :: "inputURI" :: "outputView" :: "numPartitions" :: "partitionBy" :: "persist" :: "options" :: "authentication" :: "params" :: "schemaURI" :: "schemaView" :: Nil
    val id = getOptionalValue[String]("id")
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val parsedGlob = if(!c.hasPath("inputView")) getValue[String]("inputURI") |> parseGlob("inputURI") _ else Right("")
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val timestampAsOf = getOptionalValue[String]("timestampAsOf")
    val timeTravel = readTimeTravel("options", c)
    val authentication = readAuthentication("authentication")
    val extractColumns = if(c.hasPath("schemaURI")) getValue[String]("schemaURI") |> parseURI("schemaURI") _ |> textContentForURI("schemaURI", authentication) |> getExtractColumns("schemaURI") _ else Right(List.empty)
    val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (id, name, description, parsedGlob, outputView, authentication, persist, numPartitions, partitionBy, invalidKeys, timeTravel, extractColumns, schemaView) match {
      case (Right(id), Right(name), Right(description), Right(parsedGlob), Right(outputView), Right(authentication), Right(persist), Right(numPartitions), Right(partitionBy), Right(invalidKeys), Right(timeTravel), Right(extractColumns), Right(schemaView)) =>
        val schema = if(c.hasPath("schemaView")) Left(schemaView) else Right(extractColumns)

        val stage = DeltaLakeExtractStage(
          plugin=this,
          id=id,
          name=name,
          description=description,
          input=parsedGlob,
          outputView=outputView,
          authentication=authentication,
          params=params,
          persist=persist,
          numPartitions=numPartitions,
          partitionBy=partitionBy,
          timeTravel=timeTravel,
          schema=schema
        )

        stage.stageDetail.put("input", parsedGlob)
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))

        val optionsMap = new java.util.HashMap[String, Object]()
        for (timeTravel <- timeTravel) {
          timeTravel.relativeVersion.foreach { relativeVersion => optionsMap.put("relativeVersion", java.lang.Long.valueOf(relativeVersion)) }
          timeTravel.timestampAsOf.foreach { timestampAsOf => optionsMap.put("timestampAsOf", timestampAsOf) }
          timeTravel.canReturnLastCommit.foreach { canReturnLastCommit => optionsMap.put("canReturnLastCommit", java.lang.Boolean.valueOf(canReturnLastCommit)) }
          timeTravel.versionAsOf.foreach { versionAsOf => optionsMap.put("versionAsOf", java.lang.Long.valueOf(versionAsOf)) }
        }
        stage.stageDetail.put("options", optionsMap)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(id, name, description, parsedGlob, outputView, authentication, persist, numPartitions, partitionBy, invalidKeys, timeTravel, extractColumns, schemaView).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

  def readTimeTravel(path: String, c: Config): Either[Errors, Option[TimeTravel]] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._

    def err(lineNumber: Option[Int], msg: String): Either[Errors, Option[TimeTravel]] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    if (c.hasPath(path)) {
      try {
        val config = c.getConfig(path)
        val expectedKeys = "relativeVersion" :: "timestampAsOf" :: "versionAsOf" :: "canReturnLastCommit" :: Nil
        val invalidKeys = checkValidKeys(config)(expectedKeys)
        (invalidKeys) match {
          case Right(_) => {
            (config.hasPath("relativeVersion"), config.hasPath("timestampAsOf"), config.hasPath("versionAsOf"), config.hasPath("canReturnLastCommit")) match {
              case (true, false, false, true) => {
                val relativeVersion = config.getInt("relativeVersion")
                if (relativeVersion > 0) {
                  throw new Exception(s"relativeVersion must be less than or equal to zero.")
                } else {
                  Right(Some(TimeTravel(Option(relativeVersion), None, Option(config.getBoolean("canReturnLastCommit")), None)))
                }
              }
              case (true, false, false, false) => {
                val relativeVersion = config.getInt("relativeVersion")
                if (relativeVersion > 0) {
                  throw new Exception(s"relativeVersion must be less than or equal to zero.")
                } else {
                  Right(Some(TimeTravel(Option(relativeVersion), None, None, None)))
                }
              }
              case (false, true, false, true) => Right(Some(TimeTravel(None, Option(config.getString("timestampAsOf")), Option(config.getBoolean("canReturnLastCommit")), None)))
              case (false, true, false, false) => Right(Some(TimeTravel(None, Option(config.getString("timestampAsOf")), None, None)))
              case (false, false, true, _) => Right(Some(TimeTravel(None, None, None, Option(config.getLong("versionAsOf")))))
              case (false, false, false, _) => Right(None)
              case _ => throw new Exception(s"""Only one of ${expectedKeys.mkString("['", "' ,'", "']")} is supported for time travel.""")
            }
          }
          case Left(invalidKeys) => Left(invalidKeys)
        }
      } catch {
        case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), s"Unable to read config value: ${e.getMessage}")
      }
    } else {
      Right(None)
    }
  }
}

case class TimeTravel(
  relativeVersion: Option[Int],
  timestampAsOf: Option[String],
  canReturnLastCommit: Option[Boolean],
  versionAsOf: Option[Long]
)

case class DeltaLakeExtractStage(
    plugin: DeltaLakeExtract,
    id: Option[String],
    name: String,
    description: Option[String],
    input: String,
    outputView: String,
    authentication: Option[Authentication],
    params: Map[String, String],
    persist: Boolean,
    numPartitions: Option[Int],
    partitionBy: List[String],
    timeTravel: Option[TimeTravel],
    schema: Either[String, List[ExtractColumn]]
  ) extends ExtractPipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    DeltaLakeExtractStage.execute(this)
  }
}

object DeltaLakeExtractStage {

  def execute(stage: DeltaLakeExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    // try to get the schema
    val optionSchema = try {
      ExtractUtils.getSchema(stage.schema)(spark, logger)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    CloudUtils.setHadoopConfiguration(stage.authentication)

    val df = try {
      if (arcContext.isStreaming) {
        spark.readStream.format("delta").load(stage.input)
      } else {

        // get history to allow additional logic
        val deltaLog = DeltaLog.forTable(spark, new Path(stage.input))
        val commitInfos = deltaLog.history.getHistory(None)
        val optionsMap = new java.util.HashMap[String, String]()
        var calculatedVersionAsOf: Option[Long] = None

        // determine the read options
        stage.timeTravel.foreach { timeTravel =>

          timeTravel.timestampAsOf.foreach { optionsMap.put("timestampAsOf", _) }
          timeTravel.canReturnLastCommit.foreach { canReturnLastCommit => optionsMap.put("canReturnLastCommit", canReturnLastCommit.toString) }
          timeTravel.versionAsOf.foreach { versionAsOf => optionsMap.put("versionAsOf", versionAsOf.toString) }

          // determine whether to time travel to a specific version or a calculated version
          timeTravel.relativeVersion.foreach { relativeVersion =>
            val versions = commitInfos.map { version => version.getVersion }
            val minVersion = versions.reduceLeft(_ min _)
            val maxVersion = versions.reduceLeft(_ max _)
            val maxOffset = maxVersion - minVersion
            val canReturnLastCommit = timeTravel.canReturnLastCommit.getOrElse(false)
            if (relativeVersion < (maxOffset * -1) && !canReturnLastCommit) {
              throw new Exception(s"Cannot time travel Delta table to version ${relativeVersion}. Available versions: [-${maxOffset} ... 0].")
            } else if  (relativeVersion < (maxOffset * -1) && canReturnLastCommit) {
              val calculatedVersion = minVersion
              calculatedVersionAsOf = Option(calculatedVersion)
              optionsMap.put("versionAsOf", calculatedVersion.toString)
            } else {
              val calculatedVersion = maxVersion + relativeVersion
              calculatedVersionAsOf = Option(calculatedVersion)
              optionsMap.put("versionAsOf", calculatedVersion.toString)
            }
          }
        }

        // read the data
        val df = spark.read.format("delta").options(optionsMap).load(stage.input)

        // version logging
        // this is useful for timestampAsOf as DeltaLake will extract the last timestamp EARLIER than the given timestamp
        // so the value passed in by the user is not nescessarily aligned with an actual version
        val commitInfo = stage.timeTravel match {
          case Some(timeTravel) => {
            val tt = (calculatedVersionAsOf, timeTravel.versionAsOf, timeTravel.timestampAsOf, timeTravel.canReturnLastCommit) match {
              case (Some(calculatedVersionAsOf), None, _, _) => {
                DeltaTimeTravelSpec(None, None, Some(calculatedVersionAsOf), None)
              }
              case (None, Some(versionAsOf), _, _) => {
                DeltaTimeTravelSpec(None, None, Some(versionAsOf), None)
              }
              case (None, None, Some(timestampAsOf), None) => {
                DeltaTimeTravelSpec(Some(Literal(timestampAsOf)), None, None, None)
              }
              case (None, None, Some(timestampAsOf), Some(canReturnLastCommit)) => {
                DeltaTimeTravelSpec(Some(Literal(timestampAsOf)), Some(canReturnLastCommit), None, None)
              }
              case _ => {
                throw new Exception("invalid state please raise issue.")
              }
            }
            val (version, _) = DeltaTableUtils.resolveTimeTravelVersion(spark.sessionState.conf, deltaLog, tt)
            commitInfos.filter { commit =>
              commit.getVersion == version
            }(0)
          }
          case None => commitInfos.sortBy(_.version).reverse(0)
        }

        val commitMap = new java.util.HashMap[String, Object]()
        commitMap.put("version", java.lang.Long.valueOf(commitInfo.getVersion))
        commitMap.put("timestamp", Instant.ofEpochMilli(commitInfo.getTimestamp).atZone(ZoneId.systemDefault).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
        commitInfo.operationMetrics.foreach { operationMetrics => commitMap.put("operationMetrics", operationMetrics.map { case (k, v) => (k, Try(v.toInt).getOrElse(v)) }.asJava) }
        stage.stageDetail.put("commit", commitMap)

        df
      }
    } catch {
      case e: Exception if (e.getMessage.contains("No such file or directory") && e.getMessage.contains("_delta_log")) || e.getMessage.contains("No commits found") =>
        optionSchema match {
          case Some(schema) => spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
          case None => throw new Exception(EmptySchemaExtractError(Some(stage.input)).getMessage)
        }
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    // repartition to distribute rows evenly
    val repartitionedDF = stage.partitionBy match {
      case Nil => {
        stage.numPartitions match {
          case Some(numPartitions) => df.repartition(numPartitions)
          case None => df
        }
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => df(col))
        stage.numPartitions match {
          case Some(numPartitions) => df.repartition(numPartitions, partitionCols:_*)
          case None => df.repartition(partitionCols:_*)
        }
      }
    }

    if (arcContext.immutableViews) repartitionedDF.createTempView(stage.outputView) else repartitionedDF.createOrReplaceTempView(stage.outputView)

    if (!repartitionedDF.isStreaming) {
      stage.stageDetail.put("inputFiles", Integer.valueOf(repartitionedDF.inputFiles.length))
      stage.stageDetail.put("outputColumns", Integer.valueOf(repartitionedDF.schema.length))
      stage.stageDetail.put("numPartitions", Integer.valueOf(repartitionedDF.rdd.partitions.length))

      if (stage.persist) {
        spark.catalog.cacheTable(stage.outputView, arcContext.storageLevel)
        stage.stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count))
      }
    }

    Option(repartitionedDF)
  }

}

