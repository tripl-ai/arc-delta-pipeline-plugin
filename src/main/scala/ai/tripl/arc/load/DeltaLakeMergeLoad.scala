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

class DeltaLakeMergeLoad extends PipelineStagePlugin {

  val version = ai.tripl.arc.deltalake.BuildInfo.version

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "params" :: "generateSymlinkManifest" :: "condition" :: "whenNotMatchedInsert" :: "whenMatchedUpdate" :: "whenMatchedDelete" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputURI = getValue[String]("outputURI") |> parseURI("outputURI") _
    val authentication = readAuthentication("authentication")

    // merge condition
    val condition = getValue[String]("condition")

    // read insert
    val whenNotMatchedInsert = c.hasPath("whenNotMatchedInsert") 
    val (whenNotMatchedInsertCondition, whenNotMatchedInsertValues) = if (whenNotMatchedInsert) {
      val condition = getOptionalValue[String]("whenNotMatchedInsert.condition")
      val values = if (c.hasPath("whenNotMatchedInsert.values")) {
        Option(readMap("whenNotMatchedInsert.values", c))
      } else {
        None
      }
      (condition, values)
    } else {
      (Right(None), None)
    }

    // read update
    val whenMatchedUpdate = c.hasPath("whenMatchedUpdate") 
    val whenMatchedUpdateValues = if (c.hasPath("whenMatchedUpdate.values")) {
      Option(readMap("whenMatchedUpdate.values", c))
    } else {
      None
    }

    // read delete
    val whenMatchedDelete = c.hasPath("whenMatchedDelete") 
    val whenMatchedDeleteCondition = getOptionalValue[String]("whenMatchedDelete.condition")
    val params = readMap("params", c)
    val generateSymlinkManifest = getValue[java.lang.Boolean]("generateSymlinkManifest", default = Some(true))
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, inputView, outputURI, authentication, generateSymlinkManifest, condition, whenNotMatchedInsertCondition, whenMatchedDeleteCondition, invalidKeys) match {
      case (Right(name), Right(description), Right(inputView), Right(outputURI), Right(authentication), Right(generateSymlinkManifest), Right(condition), Right(whenNotMatchedInsertCondition), Right(whenMatchedDeleteCondition), Right(invalidKeys)) =>

        val stage = DeltaLakeMergeLoadStage(
          plugin=this,
          name=name,
          description=description,
          inputView=inputView,
          outputURI=outputURI,
          authentication=authentication,
          params=params,
          generateSymlinkManifest=generateSymlinkManifest,          
          condition=condition,
          whenNotMatchedInsert=if (whenNotMatchedInsert) { Option(WhenNotMatchedInsert(whenNotMatchedInsertCondition, whenNotMatchedInsertValues)) } else None,
          whenMatchedUpdate=if (whenMatchedUpdate) { Option(WhenMatchedUpdate(whenMatchedUpdateValues)) } else None,
          whenMatchedDelete=if (whenMatchedDelete) { Option(WhenMatchedDelete(whenMatchedDeleteCondition)) } else None
        )

        // logging
        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("outputURI", outputURI.toString)
        stage.stageDetail.put("generateSymlinkManifest", generateSymlinkManifest)
        stage.stageDetail.put("condition", condition)
        if (whenNotMatchedInsert) {
          val whenNotMatchedInsertMap = new java.util.HashMap[String, Object]()
          for (whenNotMatchedInsertCondition <- whenNotMatchedInsertCondition) {
            whenNotMatchedInsertMap.put("condition", whenNotMatchedInsertCondition)
          }
          whenNotMatchedInsertValues match {
            case Some(values) => {
              whenNotMatchedInsertMap.put("values", values.asJava)
              whenNotMatchedInsertMap.put("insertAll", java.lang.Boolean.valueOf(false))
            }
            case None => whenNotMatchedInsertMap.put("insertAll", java.lang.Boolean.valueOf(true))
          }
          stage.stageDetail.put("whenNotMatchedInsert", whenNotMatchedInsertMap)
        }

        if (whenMatchedUpdate) {
          val whenMatchedUpdateMap = new java.util.HashMap[String, Object]()
          whenMatchedUpdateValues match {
            case Some(values) => {
              whenMatchedUpdateMap.put("values", values.asJava)
              whenMatchedUpdateMap.put("updateAll", java.lang.Boolean.valueOf(false))
            }
            case None => whenMatchedUpdateMap.put("updateAll", java.lang.Boolean.valueOf(true))
          }
          stage.stageDetail.put("whenNotMatchedInsert", whenMatchedUpdateMap)
        }

        if (whenMatchedDelete) {
          val whenMatchedDeleteMap = new java.util.HashMap[String, Object]()
          for (whenMatchedDeleteCondition <- whenMatchedDeleteCondition) {
            whenMatchedDeleteMap.put("condition", whenMatchedDeleteCondition)
          }
          stage.stageDetail.put("whenMatchedDelete", whenMatchedDeleteMap)
        }        

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputURI, authentication, generateSymlinkManifest, condition, whenNotMatchedInsertCondition, whenMatchedDeleteCondition, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class WhenNotMatchedInsert(
  condition: Option[String],
  values: Option[Map[String,String]]
)

case class WhenMatchedUpdate(
  values: Option[Map[String,String]]
)

case class WhenMatchedDelete(
  condition: Option[String]
)

case class DeltaLakeMergeLoadStage(
    plugin: DeltaLakeMergeLoad,
    name: String,
    description: Option[String],
    inputView: String,
    outputURI: URI,
    condition: String,
    whenNotMatchedInsert: Option[WhenNotMatchedInsert],
    whenMatchedUpdate: Option[WhenMatchedUpdate],
    whenMatchedDelete: Option[WhenMatchedDelete],
    authentication: Option[Authentication],
    params: Map[String, String],
    generateSymlinkManifest: Boolean
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    DeltaLakeMergeLoadStage.execute(this)
  }
}

object DeltaLakeMergeLoadStage {

  def execute(stage: DeltaLakeMergeLoadStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val df = spark.table(stage.inputView)

    // set write permissions
    CloudUtils.setHadoopConfiguration(stage.authentication)

    val listener = ListenerUtils.addStageCompletedListener(stage.stageDetail)

    try {
        var deltaMergeOperation = DeltaTable.forPath(stage.outputURI.toString).as("target")
          .merge(
            df.as("source"),
            stage.condition)

        // if delete
        for (whenMatchedDelete <- stage.whenMatchedDelete) {
          for (deleteCondition <- whenMatchedDelete.condition) {
              deltaMergeOperation = deltaMergeOperation.whenMatched(deleteCondition).delete()
          }
        }

        // if update
        for (whenMatchedUpdate <- stage.whenMatchedUpdate) {
          whenMatchedUpdate.values match {
            case Some(updateMap) => deltaMergeOperation = deltaMergeOperation.whenMatched().updateExpr(updateMap)
            case None => deltaMergeOperation = deltaMergeOperation.whenMatched().updateAll()
          }
        }

        // if insert
        for (whenNotMatchedInsert <- stage.whenNotMatchedInsert) {
          (whenNotMatchedInsert.condition, whenNotMatchedInsert.values) match {
            case (Some(condition), Some(values)) => deltaMergeOperation.whenNotMatched(condition).updateExpr(updateMap)
            case (Some(condition), None) => deltaMergeOperation.whenNotMatched(condition).updateAll() 
            case (None, Some(values)) => deltaMergeOperation.whenNotMatched().updateAll()
            case (None, None) =>
          }
        }

        deltaMergeOperation.execute()

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

    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    spark.sparkContext.removeSparkListener(listener)

    Option(df)
  }
}