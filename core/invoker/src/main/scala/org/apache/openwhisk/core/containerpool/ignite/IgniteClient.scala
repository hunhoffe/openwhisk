/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.containerpool.ignite

import java.io.FileNotFoundException
import java.nio.file.{Files, Paths}

import akka.actor.ActorSystem
import akka.event.Logging.{ErrorLevel, InfoLevel}
import org.apache.openwhisk.common.{Logging, LoggingMarkers, MetricEmitter, TransactionId}
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.containerpool.docker.{
  ProcessRunner,
  ProcessTimeoutException
}
import org.apache.openwhisk.core.containerpool.{ContainerAddress, ContainerId}
import pureconfig._
import pureconfig.generic.auto._

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

case class IgniteTimeoutConfig(create: Duration,
                               version: Duration,
                               inspect: Duration,
                               rm: Duration,
                               run: Duration,
                               ps: Duration)

case class IgniteClientConfig(timeouts: IgniteTimeoutConfig)

class IgniteClient(config: IgniteClientConfig = loadConfigOrThrow[IgniteClientConfig](ConfigKeys.igniteClient))(
  override implicit val executionContext: ExecutionContext,
  implicit val system: ActorSystem,
  implicit val log: Logging)
    extends IgniteApi
    with ProcessRunner {

  protected val igniteCmd: Seq[String] = {
    val alternatives = List("/usr/bin/ignite", "/usr/local/bin/ignite")

    val igniteBin = Try {
      alternatives.find(a => Files.isExecutable(Paths.get(a))).get
    } getOrElse {
      throw new FileNotFoundException(s"Couldn't locate ignite binary (tried: ${alternatives.mkString(", ")}).")
    }
    Seq(igniteBin, "-q")
  }

  // Invoke ignite CLI to determine client version.
  // If the ignite client version cannot be determined, an exception will be thrown and instance initialization will fail.
  // Rationale: if we cannot invoke `ignite version` successfully, it is unlikely subsequent `ignite` invocations will succeed.
  protected def getClientVersion(): String = {
    // hunhoffe: fix formatting.
    //TODO Ignite currently does not support formatting. So just get and log the verbatim version details
    val vf = executeProcess(igniteCmd ++ Seq("version"), config.timeouts.version)
      .andThen {
        case Success(version) => log.info(this, s"Detected ignite client version $version")
        case Failure(e) =>
          log.error(this, s"Failed to determine ignite client version: ${e.getClass} - ${e.getMessage}")
      }
    Await.result(vf, 2 * config.timeouts.version)
  }
  val clientVersion: String = getClientVersion()

  protected def runCmd(args: Seq[String], timeout: Duration)(implicit transid: TransactionId): Future[String] = {
    val cmd = igniteCmd ++ args
    val start = transid.started(
      this,
      LoggingMarkers.INVOKER_IGNITE_CMD(args.head),
      s"running ${cmd.mkString(" ")} (timeout: $timeout)",
      logLevel = InfoLevel)
    executeProcess(cmd, timeout).andThen {
      case Success(_) => transid.finished(this, start)
      case Failure(pte: ProcessTimeoutException) =>
        transid.failed(this, start, pte.getMessage, ErrorLevel)
        MetricEmitter.emitCounterMetric(LoggingMarkers.INVOKER_IGNITE_CMD_TIMEOUT(args.head))
      case Failure(t) => transid.failed(this, start, t.getMessage, ErrorLevel)
    }
  }

  override def inspectIPAddress(containerId: ContainerId)(implicit transid: TransactionId): Future[ContainerAddress] =
    // hunhoffe: change to ignite
    // This commend works: sudo ignite inspect VM db954c191491a1c1 -t "{{.Status.IPAddresses}}" 
    runCmd(
      Seq("inspect", "VM", containerId.asString, "--template", s"{{.Status.IPAddresses}}"),
      config.timeouts.inspect).flatMap {
        // hunhoffe: I don't know if '<no value>' is how ignite reports no value.
      case "<no value>" => Future.failed(new NoSuchElementException)
      case stdout       => Future.successful(ContainerAddress(stdout))
    }

  /*
  override def containerId(containerId: ContainerId)(implicit transid: TransactionId): Future[ContainerId] = {
    runCmd(Seq("inspect", "VM", containerId.asString, "--template", s"{{.ObjectMeta.UID}}"), config.timeouts.inspect)
      .flatMap {
        case "<no value>" => Future.failed(new NoSuchElementException)
        case stdout       => Future.successful(ContainerId(stdout))
      }
  }
  */

  private val importedImages = new TrieMap[String, Boolean]()
  private val importsInFlight = TrieMap[String, Future[Boolean]]()
  override def pull(image: String)(implicit transid: TransactionId): Future[Boolean] = {
    //TODO Add support for latest
    if (importedImages.contains(image)) Future.successful(true)
    else {
      importsInFlight.getOrElseUpdate(
        image, {
          val importIds = Await.result(runCmd(Seq("image", "ls", "-q"), config.timeouts.create).map(
            _.linesIterator.toSeq.map(_.trim).map(ContainerId.apply)), config.timeouts.create)
          val importNames = importIds.map(x => runCmd(Seq("inspect", "image", x.asString, "--template", s"{{.Spec.OCI}}"), 
            config.timeouts.create)).map(x => Await.result(x, config.timeouts.create))
          if (importNames.contains(image)) {
            importsInFlight.remove(image)
            importedImages.put(image, true)
            Future.successful(true)
          } else {
            runCmd(Seq("image", "import", image), config.timeouts.create)
              .map { stdout =>
                log.info(this, s"Imported image $image - $stdout")
                true
              }
                .andThen {
                  case _ =>
                    importsInFlight.remove(image)
                    importedImages.put(image, true)
                }
              }
          })
    }
  }
  
  override def run(image: String, args: Seq[String])(implicit transid: TransactionId): Future[ContainerId] = {
    runCmd(Seq("run", image) ++ args, config.timeouts.run).flatMap {
      case ""     => Future.failed(new NoSuchElementException)
      case stdout => Future.successful(ContainerId(stdout.trim))
    }
  }

  override def rm(containerId: ContainerId)(implicit transid: TransactionId): Future[Unit] =
    runCmd(Seq("vm", "rm", containerId.asString), config.timeouts.rm).map(_ => ())

  override def stop(containerId: ContainerId)(implicit transid: TransactionId): Future[Unit] =
    runCmd(Seq("vm", "stop", containerId.asString), config.timeouts.rm).map(_ => ())

  override def ps()(implicit transid: TransactionId): Future[Seq[ContainerId]] = {
    //val filter = "--template='{{.ObjectMeta.UID}}'"
    val cmd = Seq("ps")
    runCmd(cmd, config.timeouts.ps).map(_.linesIterator.toSeq.map(_.trim).map(ContainerId.apply))
  }
}

trait IgniteApi {
  protected implicit val executionContext: ExecutionContext

  def inspectIPAddress(containerId: ContainerId)(implicit transid: TransactionId): Future[ContainerAddress]

  //def containerId(containerId: ContainerId)(implicit transid: TransactionId): Future[ContainerId]

  def run(image: String, args: Seq[String])(implicit transid: TransactionId): Future[ContainerId]

  def pull(image: String)(implicit transid: TransactionId): Future[Boolean]

  def rm(containerId: ContainerId)(implicit transid: TransactionId): Future[Unit]

  def stop(containerId: ContainerId)(implicit transid: TransactionId): Future[Unit]

  def ps()(implicit transid: TransactionId): Future[Seq[ContainerId]]

  def stopAndRemove(containerId: ContainerId)(implicit transid: TransactionId): Future[Unit] = {
    for {
      _ <- stop(containerId)
      _ <- rm(containerId)
    } yield Unit
  }
}

/** Indicates any error while starting a container that leaves a broken container behind that needs to be removed */
case class BrokenIgniteContainer(id: ContainerId, msg: String) extends Exception(msg)
