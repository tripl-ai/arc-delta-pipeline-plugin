import Dependencies._

lazy val scala211 = "2.11.12"
lazy val scala212 = "2.12.10"
lazy val supportedScalaVersions = List(scala211, scala212)

lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  configs(IntegrationTest).
  settings(
    name := "arc-deltalake-pipeline-plugin",
    organization := "ai.tripl",
    organizationHomepage := Some(url("https://arc.tripl.ai")),
    crossScalaVersions := supportedScalaVersions,
    licenses := List("MIT" -> new URL("https://opensource.org/licenses/MIT")),
    scalastyleFailOnError := false,
    libraryDependencies ++= etlDeps,
    parallelExecution in Test := false,
    parallelExecution in IntegrationTest := false,
    buildInfoKeys := Seq[BuildInfoKey](version, scalaVersion),
    buildInfoPackage := "ai.tripl.arc.deltalake",
    Defaults.itSettings,
    publishTo := sonatypePublishTo.value,
    pgpPassphrase := Some(sys.env.get("PGP_PASSPHRASE").getOrElse("").toCharArray),
    pgpSecretRing := file("/pgp/secring.asc"),
    pgpPublicRing := file("/pgp/pubring.asc"),
    updateOptions := updateOptions.value.withGigahorse(false),
    resolvers += "Sonatype OSS Staging" at "https://oss.sonatype.org/content/groups/staging"
  )

// this is used to switch the unmanaged library directory so that prebuilt delta will resolve correctly (as it uses META-INF lazy loading)
def versionedUnmanagedBase(scalaVersion: String) = {
	scalaVersion match {
		case "2.11.12" => "lib_2.11"
		case "2.12.10" => "lib_2.12"
	}
}
unmanagedBase := baseDirectory.value / scalaVersion(version => versionedUnmanagedBase(version)).value

fork in run := true  

scalacOptions := Seq("-target:jvm-1.8", "-unchecked", "-deprecation")

test in assembly := {}

// META-INF discarding
assemblyMergeStrategy in assembly := {
   {
    // this match removes META-INF files except for the ones for plugins
    case PathList("META-INF", xs @ _*) =>
      xs match {
        case "services" :: xs => MergeStrategy.filterDistinctLines
        case _ => MergeStrategy.discard
      }
    case x => MergeStrategy.first
   }
}
