# Consume-Streaming-Data-flow-from-Apache-Kafka-Topics-with-Confluent-Cloud-and-Google-Databricks

<img width="992" alt="Screenshot 2022-11-16 at 15 41 12" src="https://user-images.githubusercontent.com/98153604/202210512-786619c1-2ad5-45aa-858e-adb41b8eac18.png">

## About this project

In this project, I create a streaming data pipeline for customers clickstreams data flow. The data flow is consumed by Kafka, and send to Google cloud Databricks.I used fully managed Kafka service platform Confluent Kafka generate continues fake customer clickstream data and user lookup data, I join these data flow together and directly subscribe by Google cloud databricks service. I use databricks to process this stream data flow and to finish some aggregation and load result to Google cloud storage

The project contains follow steps:

1.Confluent configuration, create a cluster, API keys
2.Create 2 new Topics
3.Connect data producers with Topics
4.Create ksqlDB cluster in confluent cloud
5.Create stream and look-up tables using ksqlDB AND Join them to create a new topic
6.New topic contain both clickstream and user information are connected to Google cloud databricks
7.data analysed using Tumbling, 




