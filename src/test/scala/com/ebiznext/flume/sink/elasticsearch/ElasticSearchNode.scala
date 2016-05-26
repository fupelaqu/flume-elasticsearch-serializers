package com.ebiznext.flume.sink.elasticsearch

import java.io.{ File, IOException }
import java.nio.file.FileVisitResult._
import java.nio.file.Files._
import java.nio.file.Paths.get
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{ FileVisitResult, Files, Path, SimpleFileVisitor }

import com.typesafe.scalalogging.slf4j.Logger
import org.elasticsearch.common.collect.Tuple
import org.elasticsearch.common.io.FileSystemUtils
import org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS
import org.elasticsearch.common.settings.{ ImmutableSettings, Settings }
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.env.Environment
import org.elasticsearch.node.internal.InternalSettingsPreparer
import org.elasticsearch.node.{ Node, NodeBuilder }
import org.elasticsearch.plugins.PluginManager
import org.slf4j.LoggerFactory

import com.ebiznext.flume.sink.elasticsearch.conf.Settings.ElasticSearch._

/**
 *
 */
trait ElasticSearchNode {

  def prepareRefresh(node: Node): Unit = {
    node.client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet()
    node.client().admin().indices().prepareRefresh().execute().actionGet()
  }

  def startEmbedded(esPath: String): Node

  def stopEmbedded(node: Node): Unit
}

trait EmbeddedElasticSearchNode extends ElasticSearchNode {

  private val logger = Logger(LoggerFactory.getLogger("esNode"))

  def startEmbedded(esPath: String = Embedded): Node = {
    logger.info(s"ElasticSearch is starting using source path '$esPath'...")
    // Prépare les plugins
    val initialSettings: Tuple[Settings, Environment] = InternalSettingsPreparer.prepareSettings(EMPTY_SETTINGS, true)
    if (!initialSettings.v2().pluginsFile().exists()) {
      FileSystemUtils.mkdirs(initialSettings.v2().pluginsFile())
      val pluginManager: PluginManager = new PluginManager(initialSettings.v2(), null, PluginManager.OutputMode.VERBOSE, TimeValue.timeValueMillis(0))
      plugins.foreach(plugin => {
        pluginManager.removePlugin(plugin)
        try {
          pluginManager.downloadAndExtract(plugin)
        } catch {
          case e: IOException =>
            logger.error(e.getMessage)
        }
      })
    }

    // Copie le jeu de données dans un répertoire temporaire
    val tmpdir: String = s"${System.getProperty("java.io.tmpdir")}${System.currentTimeMillis()}/data"
    new File(tmpdir).mkdirs()

    implicit def toPath(filename: String): Path = {
      val c = filename.charAt(0)
      if ((c == '/' || c == '\\') && c.toString != File.separator) get(filename.substring(1))
      else get(filename)
    }

    Files.walkFileTree(esPath, new SimpleFileVisitor[Path]() {
      @Override
      override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
        Files.createDirectories(tmpdir.resolve(esPath.relativize(dir)))
        CONTINUE
      }

      @Override
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        copy(file, tmpdir.resolve(esPath.relativize(file)))
        CONTINUE
      }
    })

    val esNode: Node = NodeBuilder.nodeBuilder().local(false).clusterName(Cluster).settings(
      ImmutableSettings.settingsBuilder().put("path.data", tmpdir).put("script.disable_dynamic", false)
    ).node()

    // On attend que ElasticSearch ait bien démarré
    esNode.client().admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet()

    logger.info(s"ElasticSearch is started using '$tmpdir'.")
    esNode
  }

  def stopEmbedded(node: Node): Unit = {
    logger.info("ElasticSearch is stopping...")
    if (!node.isClosed) {
      node.close()
    }
    new File(node.settings().get("path.data")).delete()
    logger.info("ElasticSearch is stopped.")
  }

}
