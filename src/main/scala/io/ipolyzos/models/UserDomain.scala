package io.ipolyzos.models

import java.sql.{Date, Timestamp}

object UserDomain {


  case class Account(id: Int, channel: String, dateOfBirth: Date, country: String)
  case class Event(accountID: Int, eventTime: Timestamp, eventTypeID: Int)
  case class EventType(eventTypeID: Int,eventTypeName: String)

  case class Subscription(id: Int,
                          accountID: Int,
                          product: String,
                          startDate: Timestamp,
                          endDate: Timestamp,
                          mrr: Option[Int],
                          quantity: Option[Int],
                          units: Option[Int],
                          bill_period_months: Int)
}
