package com.renault.hercules.traceability

import java.io.File
import java.util.concurrent.TimeUnit

//import com.sksamuel.elastic4s.ElasticsearchClientUri
//import com.sksamuel.elastic4s.http.HttpClient
import it.trenzalore.utils.config.Configurable

trait ESSparkSuite extends DataFrameSuiteBase { self: Configurable ⇒

  //  lazy val embeddedElastic: EmbeddedElastic = {
  //    EmbeddedElastic.builder()
  //      .withElasticVersion("5.6.5")
  //      .withSetting(PopularProperties.HTTP_PORT, config.getString("es.port"))
  //      .withSetting(PopularProperties.CLUSTER_NAME, "test cluster")
  //      .withInstallationDirectory(new File("tracingest/target"))
  //      .withDownloadDirectory(new File("tracingest/target"))
  //      .withCleanInstallationDirectoryOnStop(true)
  //      .withStartTimeout(90, TimeUnit.SECONDS)
  //      .build()
  //  }
  //
  //  lazy val esHttpClient = {
  //    val node = config.getString("es.nodes.url")
  //    val port = config.getString("es.port")
  //    val uri = ElasticsearchClientUri(s"elasticsearch://$node:$port")
  //    HttpClient(uri)
  //  }
  //
  //  override protected def beforeAll(): Unit = {
  //    super.beforeAll()
  //    embeddedElastic.start()
  //  }
  //
  //  override protected def afterAll(): Unit = {
  //    super.afterAll()
  //    embeddedElastic.stop()
  //  }

}
