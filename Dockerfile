FROM openjdk:8-jre-alpine

ADD target/scala-**/user-activity-analysis-assembly-0.1.jar app.jar
RUN apk update && apk add --no-cache gcompat

RUN mkdir -p /tmp
RUN chmod 777 /tmp

ENTRYPOINT ["java","-cp","/app.jar", "io.ipolyzos.UserActivityStream"]