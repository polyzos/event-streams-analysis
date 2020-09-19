package io.ipolyzos.utils

import java.sql.{Date, Timestamp}

import io.ipolyzos.models.UserDomain.{Account, Event, EventType, EventWithType, EventWithTypeAndAccount, EnrichedEvent, Subscription}
import org.apache.kafka.streams.KeyValue

trait TestDataUtils {
  lazy val accounts: List[KeyValue[String, Account]] = List(
    Account(1,"web",Date.valueOf("1978-12-23"),Some("DE")),
    Account(2,"appstore1",Date.valueOf("1940-10-05"),Some("AU")),
    Account(3,"appstore2",Date.valueOf("2004-09-17"),Some("CN")),
    Account(4,"appstore2",Date.valueOf("1948-09-02"),Some("MX")),
    Account(5,"appstore2",Date.valueOf("2004-08-23"),Some("CN")),
    Account(6,"appstore1",Date.valueOf("1991-10-10"),Some("MX")),
    Account(3,"updateStore",Date.valueOf("1975-11-20"),Some("GB")),
    Account(4,"updateStore",Date.valueOf("1960-03-24"), Some("")),
    Account(5,"updateStore",Date.valueOf("1968-08-06"), Some("")),
    Account(6,"updateStore",Date.valueOf("1994-09-19"),Some(""))
  ).map(a => new KeyValue(a.id.toString, a))

  lazy val subscriptions: List[KeyValue[String, Subscription]] = List(
    Subscription(1,1,"socialnet1",Date.valueOf("2020-02-02"),Date.valueOf("2020-03-02"),Some(9),None,None,1),
    Subscription(2,2,"socialnet2",Date.valueOf("2020-03-02"),Date.valueOf("2020-04-02"),Some(9),None,None,1),
    Subscription(3,3,"socialnet3",Date.valueOf("2020-04-02"),Date.valueOf("2020-05-02"),Some(9),None,None,1),
    Subscription(4,4,"socialnet4",Date.valueOf("2020-05-02"),Date.valueOf("2020-06-02"),Some(9),None,None,1),
    Subscription(5,5,"socialnet5",Date.valueOf("2020-01-10"),Date.valueOf("2020-02-10"),Some(9),None,None,1),
    Subscription(6,6,"socialnet6",Date.valueOf("2020-02-10"),Date.valueOf("2020-03-10"),Some(9),None,None,1),
    Subscription(3,1,"socialnet7",Date.valueOf("2020-03-10"),Date.valueOf("2020-04-10"),Some(19),None,None,1),
    Subscription(4,2,"socialnet8",Date.valueOf("2020-04-10"),Date.valueOf("2020-05-10"),Some(19),None,None,1),
    Subscription(5,3,"socialnet9",Date.valueOf("2020-05-10"),Date.valueOf("2020-06-10"),Some(19),None,None,1),
    Subscription(6,4,"socialnet10",Date.valueOf("2020-01-17"),Date.valueOf("2020-02-17"),Some(19),None,None,1)
  ).map(s => new KeyValue(s.id.toString, s))

  lazy val eventTypes: List[KeyValue[String, EventType]] = List(
    EventType(0,"post"),
    EventType(1,"newfriend"),
    EventType(2,"like"),
    EventType(3,"adview"),
    EventType(4,"dislike"),
    EventType(5,"unfriend"),
    EventType(6,"message"),
    EventType(7,"reply")
  ).map(et => new KeyValue(et.eventTypeID.toString, et))

  lazy val events: List[KeyValue[String, Event]] = List(
    Event(1,Timestamp.valueOf("2020-01-01 00:00:58"),6),
    Event(1,Timestamp.valueOf("2020-01-01 00:01:30"),6),
    Event(1,Timestamp.valueOf("2020-01-01 00:01:46"),2),
    Event(2,Timestamp.valueOf("2020-01-01 00:02:09"),3),
    Event(3,Timestamp.valueOf("2020-01-01 00:02:12"),2),
    Event(4,Timestamp.valueOf("2020-01-01 00:03:28"),6),
    Event(5,Timestamp.valueOf("2020-01-01 00:03:55"),6),
    Event(3,Timestamp.valueOf("2020-01-01 00:04:37"),2),
    Event(6,Timestamp.valueOf("2020-01-01 00:05:27"),2),
    Event(4,Timestamp.valueOf("2020-01-01 00:07:14"),7),
    Event(3,Timestamp.valueOf("2020-01-01 00:08:11"),6),
    Event(2,Timestamp.valueOf("2020-01-01 00:09:04"),2),
    Event(5,Timestamp.valueOf("2020-01-01 00:09:40"),7),
    Event(6,Timestamp.valueOf("2020-01-01 00:10:07"),3)
  ).map(e => new KeyValue(e.accountID.toString, e))

  def createConstructedEvents(): List[EnrichedEvent] = {
    events.map { event =>
      val e = event.value
      val et = eventTypes.map(_.value).filter(_.eventTypeID == e.eventTypeID).head
      val account = accounts.map(_.value).filter(_.id == e.accountID).last
      val subscription = subscriptions.map(_.value).filter(_.accountID == e.accountID).last

      val eventWithType = EventWithType(e.accountID, e.eventTime, et.eventTypeName)
      val eventWithTypeAndAccount = EventWithTypeAndAccount(account.id, account.channel, account.dateOfBirth, account.country, eventWithType.eventTime, eventWithType.eventTypeName)
      EnrichedEvent(
        eventWithTypeAndAccount.channel,
        eventWithTypeAndAccount.dateOfBirth,
        eventWithTypeAndAccount.country,
        eventWithTypeAndAccount.eventTime,
        eventWithTypeAndAccount.eventTypeName,
        subscription.id,
        subscription.accountID,
        subscription.product,
        subscription.startDate,
        subscription.endDate,
        subscription.mrr,
        subscription.quantity,
        subscription.units,
        subscription.billPeriodMonths
      )
    }
  }
}
