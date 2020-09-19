package io.ipolyzos.wrappers

import java.util.concurrent.{ExecutorService, Executors}

import akka.actor.ActorSystem
import io.ipolyzos.config.AppConfig

import scala.concurrent.ExecutionContext

trait AsyncWrapper {
  private val executorService: ExecutorService = Executors.newFixedThreadPool(AppConfig.numberOfThreads)
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executorService)

  implicit val system: ActorSystem = ActorSystem("system")
}
