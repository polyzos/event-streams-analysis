sbt clean assembly

docker build -t streams .

docker run --network=host streams