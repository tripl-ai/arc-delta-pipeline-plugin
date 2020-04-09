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
import io.delta.tables.DeltaMergeBuilder
import org.apache.spark.sql.delta._
import org.apache.hadoop.fs.Path

class DeltaLakeMergeLoad extends PipelineStagePlugin {

  val version = ai.tripl.arc.deltalake.BuildInfo.version

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "params" :: "generateSymlinkManifest" :: "condition" :: "whenMatchedDeleteFirst" :: "whenNotMatchedByTargetInsert" :: "whenNotMatchedBySourceDelete" :: "whenMatchedUpdate" :: "whenMatchedDelete" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputURI = getValue[String]("outputURI") |> parseURI("outputURI") _
    val authentication = readAuthentication("authentication")

    // merge condition
    val condition = getValue[String]("condition")
    val whenMatchedDeleteFirst = getValue[java.lang.Boolean]("whenMatchedDeleteFirst", default = Some(true))

    // read not matched by target insert
    val whenNotMatchedByTargetInsert = c.hasPath("whenNotMatchedByTargetInsert")
    val (whenNotMatchedByTargetInsertCondition, whenNotMatchedByTargetInsertValues) = if (whenNotMatchedByTargetInsert) {
      val condition = getOptionalValue[String]("whenNotMatchedByTargetInsert.condition")
      val values = if (c.hasPath("whenNotMatchedByTargetInsert.values")) {
        Option(readMap("whenNotMatchedByTargetInsert.values", c))
      } else {
        None
      }
      (condition, values)
    } else {
      (Right(None), None)
    }

    // read not matched by source delete
    val whenNotMatchedBySourceDelete = c.hasPath("whenNotMatchedBySourceDelete")
    val whenNotMatchedBySourceDeleteCondition = getOptionalValue[String]("whenNotMatchedBySourceDelete.condition")

    // read update
    val whenMatchedUpdate = c.hasPath("whenMatchedUpdate")
    val whenMatchedUpdateCondition = getOptionalValue[String]("whenMatchedUpdate.condition")
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

    (name, description, inputView, outputURI, authentication, generateSymlinkManifest, condition, whenMatchedDeleteFirst, whenNotMatchedByTargetInsertCondition, whenNotMatchedBySourceDeleteCondition, whenMatchedUpdateCondition, whenMatchedDeleteCondition, invalidKeys) match {
      case (Right(name), Right(description), Right(inputView), Right(outputURI), Right(authentication), Right(generateSymlinkManifest), Right(condition), Right(whenMatchedDeleteFirst), Right(whenNotMatchedByTargetInsertCondition), Right(whenNotMatchedBySourceDeleteCondition), Right(whenMatchedUpdateCondition), Right(whenMatchedDeleteCondition), Right(invalidKeys)) =>

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
          whenNotMatchedByTargetInsert= if (whenNotMatchedByTargetInsert) { Option(WhenNotMatchedByTargetInsert(whenNotMatchedByTargetInsertCondition, whenNotMatchedByTargetInsertValues)) } else None,
          whenNotMatchedBySourceDelete= if (whenNotMatchedBySourceDelete) { Option(WhenNotMatchedBySourceDelete(whenNotMatchedBySourceDeleteCondition)) } else None,
          whenMatchedUpdate= if (whenMatchedUpdate) { Option(WhenMatchedUpdate(whenMatchedUpdateCondition, whenMatchedUpdateValues)) } else None,
          whenMatchedDelete= if (whenMatchedDelete) { Option(WhenMatchedDelete(whenMatchedDeleteCondition)) } else None,
          whenMatchedDeleteFirst=whenMatchedDeleteFirst
        )

        // logging
        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("outputURI", outputURI.toString)
        stage.stageDetail.put("generateSymlinkManifest", generateSymlinkManifest)
        stage.stageDetail.put("condition", condition)
        stage.stageDetail.put("whenMatchedDeleteFirst", whenMatchedDeleteFirst)

        if (whenNotMatchedByTargetInsert) {
          val whenNotMatchedByTargetInsertMap = new java.util.HashMap[String, Object]()
          whenNotMatchedByTargetInsertCondition.foreach{ whenNotMatchedByTargetInsertMap.put("condition", _) }
          whenNotMatchedByTargetInsertValues match {
            case Some(values) => {
              whenNotMatchedByTargetInsertMap.put("values", values.asJava)
              whenNotMatchedByTargetInsertMap.put("insertAll", java.lang.Boolean.valueOf(false))
            }
            case None => whenNotMatchedByTargetInsertMap.put("insertAll", java.lang.Boolean.valueOf(true))
          }
          stage.stageDetail.put("whenNotMatchedByTargetInsert", whenNotMatchedByTargetInsertMap)
        }

        if (whenNotMatchedBySourceDelete) {
          val whenNotMatchedBySourceDeleteMap = new java.util.HashMap[String, Object]()
          whenNotMatchedBySourceDeleteCondition.foreach{ whenNotMatchedBySourceDeleteMap.put("condition", _) }
          stage.stageDetail.put("whenNotMatchedBySourceDelete", whenNotMatchedBySourceDeleteMap)
        }

        if (whenMatchedUpdate) {
          val whenMatchedUpdateMap = new java.util.HashMap[String, Object]()
          whenMatchedUpdateCondition.foreach{ whenMatchedUpdateMap.put("condition", _) }
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
          whenMatchedDeleteCondition.foreach{ whenMatchedDeleteMap.put("condition", _) }
          stage.stageDetail.put("whenMatchedDelete", whenMatchedDeleteMap)
        }

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputURI, authentication, generateSymlinkManifest, condition, whenMatchedDeleteFirst, whenNotMatchedByTargetInsertCondition, whenNotMatchedBySourceDeleteCondition, whenMatchedUpdateCondition, whenMatchedDeleteCondition, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class WhenNotMatchedByTargetInsert(
  condition: Option[String],
  values: Option[Map[String,String]]
)

case class WhenNotMatchedBySourceDelete(
  condition: Option[String]
)

case class WhenMatchedUpdate(
  condition: Option[String],
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
    whenMatchedDeleteFirst: Boolean,
    whenNotMatchedByTargetInsert: Option[WhenNotMatchedByTargetInsert],
    whenNotMatchedBySourceDelete: Option[WhenNotMatchedBySourceDelete],
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

    def whenMatchedDeleteCondition(deltaMergeOperation: DeltaMergeBuilder): DeltaMergeBuilder = {
      stage.whenMatchedDelete match {
        case Some(whenMatchedDelete) =>
          whenMatchedDelete.condition match {
            case Some(condition) => deltaMergeOperation.whenMatched(condition).delete
            case None => deltaMergeOperation.whenMatched.delete
          }
        case None => deltaMergeOperation
      }
    }

    def whenMatchedUpdateCondition(deltaMergeOperation: DeltaMergeBuilder): DeltaMergeBuilder = {
      stage.whenMatchedUpdate match {
        case Some(whenMatchedUpdate) =>
          (whenMatchedUpdate.condition, whenMatchedUpdate.values) match {
            case (Some(condition), Some(values)) => deltaMergeOperation.whenMatched(condition).updateExpr(values)
            case (Some(condition), None) => deltaMergeOperation.whenMatched(condition).updateAll
            case (None, Some(values)) => deltaMergeOperation.whenMatched.updateExpr(values)
            case (None, None) => deltaMergeOperation.whenMatched.updateAll
          }
        case None => deltaMergeOperation
      }  
    }  

    val df = spark.table(stage.inputView)

    // set write permissions
    CloudUtils.setHadoopConfiguration(stage.authentication)

    try {

        // build the operation
        var deltaMergeOperation: DeltaMergeBuilder = DeltaTable.forPath(stage.outputURI.toString).as("target")
          .merge(
            df.as("source"),
            stage.condition)

        // match
        deltaMergeOperation = if (stage.whenMatchedDeleteFirst) {
          val deltaMergeOperationWithDelete = whenMatchedDeleteCondition(deltaMergeOperation)
          whenMatchedUpdateCondition(deltaMergeOperationWithDelete)
        } else {
          val deltaMergeOperationWithUpdate = whenMatchedUpdateCondition(deltaMergeOperation)
          whenMatchedDeleteCondition(deltaMergeOperationWithUpdate)
        }

        // if insert as source rows dont exist in target dataset
        for (whenNotMatchedByTargetInsert <- stage.whenNotMatchedByTargetInsert) {
          (whenNotMatchedByTargetInsert.condition, whenNotMatchedByTargetInsert.values) match {
            case (Some(condition), Some(values)) => deltaMergeOperation = deltaMergeOperation.whenNotMatchedByTarget(condition).insertExpr(values)
            case (Some(condition), None) => deltaMergeOperation = deltaMergeOperation.whenNotMatchedByTarget(condition).insertAll
            case (None, Some(values)) => deltaMergeOperation = deltaMergeOperation.whenNotMatchedByTarget.insertExpr(values)
            case (None, None) => deltaMergeOperation = deltaMergeOperation.whenNotMatchedByTarget.insertAll
          }
        }

        // if delete as target rows dont exist in source dataset
        for (whenNotMatchedBySourceDelete <- stage.whenNotMatchedBySourceDelete) {
          whenNotMatchedBySourceDelete.condition match {
            case Some(condition) => deltaMergeOperation = deltaMergeOperation.whenNotMatchedBySource(condition).delete
            case None => deltaMergeOperation = deltaMergeOperation.whenNotMatchedBySource.delete
          }
        }

        // execute
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

    Option(df)
  }
}