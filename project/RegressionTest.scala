import sbt.Keys._
import sbt.{Def, _}

object RegressionTest {
  lazy val RegressionTestConfig: Configuration = config("regression") extend IntegrationTest.IntegrationTestConfig

  lazy val RegressionTestSettings: Seq[Def.Setting[_]] = inConfig(RegressionTestConfig)(Defaults.testSettings) ++ Seq(
    scalaSource in RegressionTestConfig := baseDirectory.value / "src/test/regression/scala",
    resourceDirectory in RegressionTestConfig := baseDirectory.value / "src/test/regression/resources",
    fork in RegressionTestConfig := true,
    parallelExecution in RegressionTestConfig := false,
    javaOptions in RegressionTestConfig ++= Seq(
      "-Dcom.sun.management.jmxremote",
      "-Dcom.sun.management.jmxremote.port=9191",
      "-Dcom.sun.management.jmxremote.rmi.port=9191",
      "-Dcom.sun.management.jmxremote.authenticate=false",
      "-Dcom.sun.management.jmxremote.ssl=false",
      "-Djava.rmi.server.hostname=localhost"
    )
  )
}
