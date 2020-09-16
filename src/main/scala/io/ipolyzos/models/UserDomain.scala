package io.ipolyzos.models

import java.sql.{Date, Timestamp}

object UserDomain {


  case class Account(id: Int, channel: String, dateOfBirth: Date, country: Option[String])
  case class Event(accountID: Int, eventTime: Timestamp, eventTypeID: Int)
  case class EventType(eventTypeID: Int, eventTypeName: String)
  case class Subscription(id: Int,
                          accountID: Int,
                          product: String,
                          startDate: Date,
                          endDate: Date,
                          mrr: Option[Int],
                          quantity: Option[Int],
                          units: Option[Int],
                          billPeriodMonths: Int)

  case class EventWithType(accountID: Int, eventTime: Timestamp, eventTypeName: String)
  case class EventWithTypeAndAccount(accountID: Int, channel: String, dateOfBirth: Date, country: Option[String], eventTime: Timestamp, eventTypeName: String)
  case class EventWithTypeAndAccountAndSubscription(channel: String,
                                                    dateOfBirth: Date,
                                                    country: Option[String],
                                                    eventTime: Timestamp,
                                                    eventTypeName: String,
                                                    subscriptionID: Int,
                                                    accountID: Int,
                                                    product: String,
                                                    subscriptionStartDate: Date,
                                                    subscriptionEndDate: Date,
                                                    mrr: Option[Int],
                                                    quantity: Option[Int],
                                                    units: Option[Int],
                                                    billPeriodMonths: Int)
}
