package io.ipolyzos.formatters

import java.sql.{Date, Timestamp}

import io.circe.Decoder.Result
import io.circe.syntax.EncoderOps

object CustomFormatters {
  import io.circe._
  implicit val TimestampFormat: Encoder[Timestamp] with Decoder[Timestamp] = new Encoder[Timestamp] with Decoder[Timestamp] {
    override def apply(date: Timestamp): Json = Encoder.encodeLong.apply(date.getTime)

    override def apply(c: HCursor): Result[Timestamp] = Decoder.decodeLong.map(s => new Timestamp(s)).apply(c)
  }

  implicit val encoderOptionalString: Encoder[Option[String]] = {
    case Some(amount) => amount.asJson
    case None => Json.obj("waived" -> true.asJson)
  }

  implicit val DateFormat: Encoder[Date] with Decoder[Date] = new Encoder[Date] with Decoder[Date] {
    override def apply(date: Date): Json = Encoder.encodeLong.apply(date.getTime)

    override def apply(c: HCursor): Result[Date] = Decoder.decodeLong.map(s => new Date(s)).apply(c)
  }
}
