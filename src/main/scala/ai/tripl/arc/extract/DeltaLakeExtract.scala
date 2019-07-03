package ai.tripl.arc.extract

import java.io._
import java.net.URI
import java.util.Properties

import scala.annotation.tailrec
import scala.collection.JavaConverters._

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
    val options = readMap("options", c)
    val authentication = readAuthentication("authentication")
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)    

    (name, description, parsedGlob, outputView, authentication, persist, numPartitions, partitionBy, invalidKeys) match {
      case (Right(name), Right(description), Right(parsedGlob), Right(outputView), Right(authentication), Right(persist), Right(numPartitions), Right(partitionBy), Right(invalidKeys)) => 

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
          options=options
        )

        stage.stageDetail.put("input", parsedGlob) 
        stage.stageDetail.put("outputView", outputView)  
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))                       
        stage.stageDetail.put("options", options.asJava)  

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, parsedGlob, outputView, authentication, persist, numPartitions, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

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
    options: Map[String, String]
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
        spark.readStream.format("delta").options(stage.options).load(stage.input) 
      } else {
        spark.read.format("delta").options(stage.options).load(stage.input) 
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

