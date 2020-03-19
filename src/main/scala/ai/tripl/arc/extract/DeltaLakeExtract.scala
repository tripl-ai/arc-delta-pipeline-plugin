package ai.tripl.arc.extract

import java.io._
import java.net.URI
import java.util.Properties
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

import scala.annotation.tailrec
import scala.collection.JavaConverters._

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

class DeltaLakeExtract extends PipelineStagePlugin {

  val version = ai.tripl.arc.deltalake.BuildInfo.version

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "outputView" :: "numPartitions" :: "partitionBy" :: "persist" :: "options" :: "authentication" :: "params" :: Nil
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
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, parsedGlob, outputView, authentication, persist, numPartitions, partitionBy, invalidKeys, timeTravel) match {
      case (Right(name), Right(description), Right(parsedGlob), Right(outputView), Right(authentication), Right(persist), Right(numPartitions), Right(partitionBy), Right(invalidKeys), Right(timeTravel)) =>

        val stage = DeltaLakeExtractStage(
          plugin=this,
          name=name,
          description=description,
          input=parsedGlob,
          outputView=outputView,
          authentication=authentication,
          params=params,
          persist=persist,
          numPartitions=numPartitions,
          partitionBy=partitionBy,
          timeTravel=timeTravel
        )

        stage.stageDetail.put("input", parsedGlob)
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))

        val optionsMap = new java.util.HashMap[String, Object]()
        for (timeTravel <- timeTravel) {
          for (relativeVersion <- timeTravel.relativeVersion) {
            optionsMap.put("relativeVersion", java.lang.Long.valueOf(relativeVersion))
            stage.stageDetail.put("options", optionsMap)
          }
          for (timestampAsOf <- timeTravel.timestampAsOf) {
            optionsMap.put("timestampAsOf", timestampAsOf)
            stage.stageDetail.put("options", optionsMap)
          }
          for (versionAsOf <- timeTravel.versionAsOf) {
            optionsMap.put("versionAsOf", java.lang.Long.valueOf(versionAsOf))
            stage.stageDetail.put("options", optionsMap)
          }
        }

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, parsedGlob, outputView, authentication, persist, numPartitions, partitionBy, invalidKeys, timeTravel).collect{ case Left(errs) => errs }.flatten
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
        val expectedKeys = "relativeVersion" :: "timestampAsOf" :: "versionAsOf" :: Nil
        val invalidKeys = checkValidKeys(config)(expectedKeys)
        (invalidKeys) match {
          case Right(_) => {
            (config.hasPath("relativeVersion"), config.hasPath("timestampAsOf"), config.hasPath("versionAsOf")) match {
              case (true, false, false) => {
                val relativeVersion = config.getInt("relativeVersion")
                if (relativeVersion > 0) {
                  throw new Exception(s"relativeVersion must be less than or equal to zero.")
                } else {
                  Right(Some(TimeTravel(Option(relativeVersion), None, None)))
                }
              }
              case (false, true, false) => Right(Some(TimeTravel(None, Option(config.getString("timestampAsOf")), None)))
              case (false, false, true) => Right(Some(TimeTravel(None, None, Option(config.getInt("versionAsOf")))))
              case (false, false, false) => Right(None)
              case _ => throw new Exception(s"Only one of ['relativeVersion', 'timestampAsOf', 'versionAsOf'] is supported for time travel.")
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
  versionAsOf: Option[Int]
)

case class DeltaLakeExtractStage(
    plugin: DeltaLakeExtract,
    name: String,
    description: Option[String],
    input: String,
    outputView: String,
    authentication: Option[Authentication],
    params: Map[String, String],
    persist: Boolean,
    numPartitions: Option[Int],
    partitionBy: List[String],
    timeTravel: Option[TimeTravel]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    DeltaLakeExtractStage.execute(this)
  }
}

object DeltaLakeExtractStage {

  def execute(stage: DeltaLakeExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    CloudUtils.setHadoopConfiguration(stage.authentication)

    val df = try {
      if (arcContext.isStreaming) {
        spark.readStream.format("delta").load(stage.input)
      } else {

        // get history to allow additional logic
        val deltaLog = DeltaLog.forTable(spark, new Path(stage.input))
        val commitInfos = deltaLog.history.getHistory(None)
        val optionsMap = new java.util.HashMap[String, String]()

        // determine the read options
        for (timeTravel <- stage.timeTravel) {
          for (timestampAsOf <- timeTravel.timestampAsOf) {
            optionsMap.put("timestampAsOf", timestampAsOf)
          }
          for (versionAsOf <- timeTravel.versionAsOf) {
            optionsMap.put("versionAsOf", versionAsOf.toString)
          }

          // determine whether to time travel to a specific version or a calculated version
          for (relativeVersion <- timeTravel.relativeVersion) {
            val versions = commitInfos.map { version => version.getVersion }
            val minVersion = versions.reduceLeft(_ min _)
            val maxVersion = versions.reduceLeft(_ max _)
            val maxOffset = maxVersion - minVersion

            if (relativeVersion < (maxOffset * -1)) {
                throw new Exception(s"Cannot time travel Delta table to version ${relativeVersion}. Available versions: [-${maxOffset} ... 0].")
            } else {
              val calculatedVersion = maxVersion + relativeVersion
              optionsMap.put("versionAsOf", calculatedVersion.toString)
            }
          }
        }

        // read the data
        val df = spark.read.format("delta").options(optionsMap).load(stage.input)

        // version logging
        // this is useful for timestampAsOf as DeltaLake will extract the last timestamp EARLIER than the given timestamp
        // so the value passed in by the user is not nescessarily aligned with an actual version
        val commitInfo = (optionsMap.asScala.get("versionAsOf"), optionsMap.asScala.get("timestampAsOf")) match {
          case (None, None) => {
            commitInfos.sortBy(_.version).reverse(0)
          }
          case (Some(versionAsOf), None) => {
            val tt = DeltaTimeTravelSpec(None, Some(versionAsOf.toLong), None)
            val (version, _) = DeltaTableUtils.resolveTimeTravelVersion(spark.sessionState.conf, deltaLog, tt)
            commitInfos.filter { commit =>
              commit.getVersion == version
            }(0)
          }
          case (None, Some(timestampAsOf)) => {
            val tt = DeltaTimeTravelSpec(Some(Literal(timestampAsOf)), None, None)
            val (version, _) = DeltaTableUtils.resolveTimeTravelVersion(spark.sessionState.conf, deltaLog, tt)
            commitInfos.filter { commit =>
              commit.getVersion == version
            }(0)
          }
          case (Some(_), Some(_)) => {
            throw new Exception("invalid state please raise issue.")
          }
        }
        val commitMap = new java.util.HashMap[String, Object]()
        commitMap.put("version", java.lang.Long.valueOf(commitInfo.getVersion))
        commitMap.put("timestamp", Instant.ofEpochMilli(commitInfo.getTimestamp).atZone(ZoneId.systemDefault).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
        stage.stageDetail.put("commit", commitMap)

        df
      }
    } catch {
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
    repartitionedDF.createOrReplaceTempView(stage.outputView)

    if (!repartitionedDF.isStreaming) {
      stage.stageDetail.put("inputFiles", Integer.valueOf(repartitionedDF.inputFiles.length))
      stage.stageDetail.put("outputColumns", Integer.valueOf(repartitionedDF.schema.length))
      stage.stageDetail.put("numPartitions", Integer.valueOf(repartitionedDF.rdd.partitions.length))

      if (stage.persist) {
        repartitionedDF.persist(arcContext.storageLevel)
        stage.stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count))
      }
    }

    Option(repartitionedDF)
  }

}

