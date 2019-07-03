import sbt._

object Dependencies {
  // versions
  lazy val sparkVersion = "2.4.3"
  lazy val hadoopVersion = "2.9.2"

  // testing
  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.7" % "test,it"
  val hadoopCommon =  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "it"
  val hadoopAWS = "org.apache.hadoop" % "hadoop-aws" % hadoopVersion % "it"

  // arc
  val arc = "ai.tripl" %% "arc" % "2.0.0" % "provided"
  val typesafeConfig = "com.typesafe" % "config" % "1.3.1" intransitive()  

  // spark
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

  // kafka
  val deltaCore = "io.delta" %% "delta-core" % "0.2.0"

  // Project
  val etlDeps = Seq(
    scalaTest,
    hadoopCommon,
    hadoopAWS,
    
    arc,
    typesafeConfig,

    sparkSql,
    
    deltaCore
  )
}