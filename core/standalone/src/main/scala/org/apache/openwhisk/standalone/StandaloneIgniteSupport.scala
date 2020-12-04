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

package org.apache.openwhisk.standalone

import java.io.FileNotFoundException
import java.net.{ServerSocket, Socket}
import java.nio.file.{Files, Paths}

import akka.Done
import akka.actor.{ActorSystem, CoordinatedShutdown}
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.containerpool.ignite.{
  BrokenIgniteContainer,
  IgniteClient,
  IgniteClientConfig,
}
import org.apache.openwhisk.core.containerpool.{ContainerAddress, ContainerId}
import pureconfig._
import pureconfig.generic.auto._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.sys.process._
import scala.util.Try

class StandaloneIgniteSupport(ignite: IgniteClient)(implicit logging: Logging,
                                                    ec: ExecutionContext,
                                                    actorSystem: ActorSystem) {
  CoordinatedShutdown(actorSystem)
    .addTask(
      CoordinatedShutdown.PhaseBeforeActorSystemTerminate,
      "cleanup containers launched for Standalone Server support") { () =>
      cleanup()
      Future.successful(Done)
    }

  def cleanup(): Unit = {
    implicit val transid = TransactionId(TransactionId.systemPrefix + "standalone")
    val cleaning =
      // hunhoffe: TODO: add filters back in.
      // ignite.ps(filters = Seq("name" -> StandaloneIgniteSupport.prefix), all = true).flatMap { containers =>
      ignite.ps().flatMap { containers =>
        logging.info(this, s"removing ${containers.size} containers launched for Standalone server support.")
        val removals = containers.map { id =>
          ignite.stopAndRemove(id)
        }
        Future.sequence(removals)
      }
    Await.ready(cleaning, 30.seconds)
  }
}

object StandaloneIgniteSupport {
  val prefix = "whisk-"

  def checkOrAllocatePort(preferredPort: Int): Int = {
    if (isPortFree(preferredPort)) preferredPort else freePort()
  }

  private def freePort(): Int = {
    val socket = new ServerSocket(0)
    try socket.getLocalPort
    finally if (socket != null) socket.close()
  }

  def isPortFree(port: Int): Boolean = {
    Try(new Socket("localhost", port).close()).isFailure
  }

  def createRunCmd(name: String,
                   environment: Map[String, String] = Map.empty,
                   igniteRunParameters: Map[String, Set[String]] = Map.empty): Seq[String] = {
    val environmentArgs = environment.flatMap {
      case (key, value) => Seq("-e", s"$key=$value")
    }

    val params = igniteRunParameters.flatMap {
      case (key, valueList) => valueList.toList.flatMap(Seq(key, _))
    }

    Seq("--name", name) ++
      environmentArgs ++ params
  }

  def containerName(name: String) = {
    prefix + name
  }

  /**
   * Returns the hostname to access the playground.
   * It defaults to localhost but it can be overriden
   * and it is useful when the standalone is run in a container.
   */
  def getExternalHostName(): String = {
    sys.props.get("whisk.standalone.host.external").getOrElse(getLocalHostName())
  }

  /**
   * Returns the address to be used by code running outside of container to connect to
   * server. On non linux setups its 'localhost'. However for Linux setups its the ip used
   * by docker for docker0 network to refer to host system
   */
  def getLocalHostName(): String = {
    sys.props
      .get("whisk.standalone.host.name")
      .getOrElse(hostIpLinux)
  }

  def getLocalHostIp(): String = {
    sys.props
      .get("whisk.standalone.host.ip")
      .getOrElse(hostIpLinux)
  }

  /**
   * Determines the name/ip which code running within container can use to connect back to Controller
   */
  def getLocalHostInternalName(): String = {
    sys.props
      .get("whisk.standalone.host.internal")
      .getOrElse(hostIpLinux)
  }

  def prePullImage(imageName: String)(implicit logging: Logging): Unit = {
    //docker images openwhisk/action-nodejs-v10:1.16.0
    //REPOSITORY                    TAG                 IMAGE ID            CREATED             SIZE
    //openwhisk/action-nodejs-v10   1.16.0              dbb0f8e1a050        5 days ago          967MB
    val imageResult = s"$igniteCmd images $imageName".!!
    val imageExist = imageResult.linesIterator.toList.size > 1
    if (!imageExist || imageName.contains(":1.16.0")) {
      logging.info(this, s"Ignite Pre pulling $imageName")
      s"$igniteCmd image import $imageName".!!
    }
  }

  private lazy val hostIpLinux: String = {
    //Gets the hostIp for linux https://github.com/docker/for-linux/issues/264#issuecomment-387525409
    // Typical output would be like and we need line with default
    // $ docker run --rm alpine ip route
    // default via 172.17.0.1 dev eth0
    // 172.17.0.0/16 dev eth0 scope link  src 172.17.0.2
    // hunhoffe: TODO: messy to still use docker command....
    val cmdResult = s"docker run --rm alpine ip route".!!
    cmdResult.linesIterator
      .find(_.contains("default"))
      .map(_.split(' ').apply(2).trim)
      .getOrElse(throw new IllegalStateException(s"'ip route' result did not match expected output - \n$cmdResult"))
  }

  private lazy val igniteCmd = {
    //TODO Logic duplicated from DockerClient and WindowsDockerClient for now
    val executable = loadConfig[String]("whisk.ignite.executable").toOption
    val alternatives = List("/usr/bin/ignite", "/usr/local/bin/ignite")
    Try {
      alternatives.find(a => Files.isExecutable(Paths.get(a))).get
    } getOrElse {
      throw new FileNotFoundException(s"Couldn't locate ignite binary (tried: ${alternatives.mkString(", ")}).")
    }
  }
}

class StandaloneIgniteClient(pullDisabled: Boolean)(implicit log: Logging, as: ActorSystem, ec: ExecutionContext)
    extends IgniteClient()(ec, as, log) {

  override def pull(image: String)(implicit transid: TransactionId): Future[Unit] = {
    if (pullDisabled) Future.successful(()) else super.pull(image)
  }

  override def runCmd(args: Seq[String], timeout: Duration)(implicit transid: TransactionId): Future[String] =
    super.runCmd(args, timeout)

  val clientConfig: IgniteClientConfig = loadConfigOrThrow[IgniteClientConfig](ConfigKeys.igniteClient)

  def runDetached(image: String, args: Seq[String], shouldPull: Boolean)(
    implicit tid: TransactionId): Future[StandaloneIgniteContainer] = {
    for {
      _ <- if (shouldPull) pull(image) else Future.successful(())
      id <- run(image, args).recoverWith {
        case t @ BrokenIgniteContainer(brokenId, _) =>
          // Remove the broken container - but don't wait or check for the result.
          // If the removal fails, there is nothing we could do to recover from the recovery.
          rm(brokenId)
          Future.failed(t)
        case t => Future.failed(t)
      }
      ip <- inspectIPAddress(id).recoverWith {
        // remove the container immediately if inspect failed as
        // we cannot recover that case automatically
        case e =>
          rm(id)
          Future.failed(e)
      }
    } yield StandaloneIgniteContainer(id, ip)
  }
}

case class StandaloneIgniteContainer(id: ContainerId, addr: ContainerAddress)
