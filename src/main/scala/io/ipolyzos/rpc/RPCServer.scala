package io.ipolyzos.rpc


import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpRequest, StatusCodes}
import akka.util.ByteString
import io.circe.Printer
import io.circe.parser.decode
import io.ipolyzos.models.UserDomain.{Account, Subscription}
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.{KafkaStreams, KeyQueryMetadata, StoreQueryParameters}
import org.apache.kafka.streams.state.{HostInfo, QueryableStoreTypes, ReadOnlyKeyValueStore}

import scala.concurrent.{Await, ExecutionContextExecutor, Future}

case class RPCServer(private val streams: KafkaStreams, private val hostInfo: HostInfo)(implicit system: ActorSystem) {
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  private var isActive: Boolean = false

  import io.circe.syntax._
  import io.circe.generic.auto._
  import io.ipolyzos.formatters.CustomFormatters._

  import scala.concurrent.duration._
  import akka.http.scaladsl.server.Directives._
  import collection.JavaConverters._

  case class OnErrorResponse(status: String, message: String)

  private val accountRoutes =
    path("accounts") {
      pathEndOrSingleSlash {
        println(s"Requesting data for all account keys.")
        complete(StatusCodes.OK)
      }
    } ~
    path("accounts" / IntNumber) { (accountID: Int) =>
      get {
        val metadata: KeyQueryMetadata = streams.queryMetadataForKey("accountStore", accountID.toString, Serdes.String.serializer())
        if (metadata.getActiveHost.host() == hostInfo.host() && metadata.getActiveHost.port() == hostInfo.port()) {
          val accountResult: Account = retrieveValueForKey[Account](accountID.toString, "accountStore")
          if (accountResult == null) {
            complete(HttpEntity(
              ContentTypes.`application/json`,
              OnErrorResponse(StatusCodes.NotFound.toString(), s"Failed to retrieve value for account with id: $accountID").asJson.printWith(Printer.noSpaces).getBytes()
            ))
          } else {
            complete(
              HttpEntity(
                ContentTypes.`application/json`,
                accountResult.asJson.printWith(Printer.noSpaces).getBytes()
              )
            )
          }

        } else {
          val accountResponse = Await.result(retrieveValueForAccountRemote(metadata, accountID.toString), 1 minutes)
          if (accountResponse == null) {
            complete(HttpEntity(
              ContentTypes.`application/json`,
              OnErrorResponse(StatusCodes.NotFound.toString(), s"Failed to retrieve value for account with id: $accountID").asJson.printWith(Printer.noSpaces).getBytes()
            ))
          } else {
            complete(
              HttpEntity(
                ContentTypes.`application/json`,
                accountResponse.asJson.printWith(Printer.noSpaces).getBytes()
              ))
          }
        }
      }
    }


  private val subscriptionRoutes =
    path("subscriptions") {
      pathEndOrSingleSlash {
        println(s"Requesting data for all subscription keys.")
        complete(StatusCodes.OK)
      }
    } ~
      path("subscriptions" / IntNumber) { (subscriptionID: Int) =>
        get {
          val metadata: KeyQueryMetadata = streams.queryMetadataForKey("subscriptionStore", subscriptionID.toString, Serdes.String.serializer())
          if (metadata.getActiveHost.host() == hostInfo.host() && metadata.getActiveHost.port() == hostInfo.port()) {
            val subscriptionResult = retrieveValueForKey[Subscription](subscriptionID.toString, "subscriptionStore")
            if (subscriptionResult == null) {
              complete(HttpEntity(
                ContentTypes.`application/json`,
                OnErrorResponse(StatusCodes.NotFound.toString(), s"Failed to retrieve value for account with id: $subscriptionID").asJson.noSpaces.getBytes()
              ))
            } else {
              complete(
                HttpEntity(
                  ContentTypes.`application/json`,
                  subscriptionResult.asJson.printWith(Printer.noSpaces).getBytes()
                )
              )
            }

          } else {
            val subscriptionResponse = Await.result(retrieveValueForSubscriptionsRemote(metadata, subscriptionID.toString), 1 minutes)
            if (subscriptionResponse == null) {
              complete(HttpEntity(
                ContentTypes.`application/json`,
                OnErrorResponse(StatusCodes.NotFound.toString(), s"Failed to retrieve value for subscription with id: $subscriptionID").asJson.printWith(Printer.noSpaces).getBytes()
              ))
            } else {
              complete(
                HttpEntity(
                  ContentTypes.`application/json`,
                  subscriptionResponse.asJson.printWith(Printer.noSpaces).getBytes()
                ))
            }
          }
        }
      }

  private val routes = accountRoutes ~ subscriptionRoutes

  def start(): Unit = {
    println(s"Starting RPC Server at ${hostInfo.host()}:${hostInfo.port()}")
    Http().newServerAt(hostInfo.host(), hostInfo.port()).bind(routes)
  }

  def stop = {
    system.terminate()
  }

  def setActive(state: Boolean) = {
    isActive = state
  }

  private def retrieveValueForKey[T](key: String, storeName: String): T = {
    val accountStore: ReadOnlyKeyValueStore[String, T] = streams.store(
                StoreQueryParameters.fromNameAndType(
                  storeName,
                  QueryableStoreTypes.keyValueStore[String, T]()
                )
              )

    accountStore.get(key)
  }

  private def retrieveValueForAccountRemote(metadata: KeyQueryMetadata, id: String): Future[Account] = {
    val uri = s"http://${metadata.getActiveHost.host()}:${metadata.getActiveHost.port()}/accounts/$id"
    val response = Http().singleRequest(HttpRequest(uri = uri))
    response.flatMap { r =>
      r.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map { body =>
        val account = decode[Account](body.utf8String).right.getOrElse(null)
        account
      }
    }
  }

  private def retrieveValueForSubscriptionsRemote(metadata: KeyQueryMetadata, id: String): Future[Subscription] = {
    val uri = s"http://${metadata.getActiveHost.host()}:${metadata.getActiveHost.port()}/subscriptions/$id"
    val response = Http().singleRequest(HttpRequest(uri = uri))
    response.flatMap { r =>
      r.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map { body =>
        val account = decode[Subscription](body.utf8String).right.getOrElse(null)
        account
      }
    }
  }
}
