# Consume-Streaming-Data-flow-from-Apache-Kafka-Topics-with-Confluent-Cloud-and-Google-Databricks

<img width="992" alt="Screenshot 2022-11-16 at 15 41 12" src="https://user-images.githubusercontent.com/98153604/202210512-786619c1-2ad5-45aa-858e-adb41b8eac18.png">

## About this project

In this project, I create a streaming data pipeline for customers clickstreams data flow. The data flow is consumed by Kafka, and send to Google cloud Databricks.I used fully managed Kafka service platform Confluent Kafka generate continues fake customer clickstream data and user lookup data, I join these data flow together and directly subscribe by Google cloud databricks service. I use databricks to process this stream data flow and to finish some aggregation and load result to Google cloud storage

The project contains follow steps:

1.Confluent cloud configuration, create a cluster, API keys

2.Create 2 new Topics

3.Connect data producers with Topics

4.Create ksqlDB cluster in confluent cloud

5.Create stream and look-up tables using ksqlDB AND Join them to create a new topic

6.New topic contain both clickstream and user information are connected to Google cloud databricks

7.data analysed using Tumbling, Sliding and Session Window for different aggregation analysis

## 1. Register Confluent Cloud account, and do some configuration in Confluent cloud, create a cluster and Api keys

Confluent cloud is an platform for managed kafka service

![image](https://user-images.githubusercontent.com/98153604/202293662-65c6985e-7517-4f7c-911a-faafe9d702d4.png)
Select Bacis level:
![image](https://user-images.githubusercontent.com/98153604/202293722-524f47ae-a94b-4ff3-8f0c-f308637d4a17.png)

Choose Google cloud and region, zone, help this fully managed kafka service held in different cloud platform

![image](https://user-images.githubusercontent.com/98153604/202294170-383f9910-f02e-48c6-be6f-6a2a1ea5f5c5.png)

![image](https://user-images.githubusercontent.com/98153604/202294290-9e1cfa5c-ec46-4fcf-b208-bea6b051ba52.png)


![image](https://user-images.githubusercontent.com/98153604/202294359-d98c534c-9ee2-4135-80c0-c2ae39b10ec6.png)

![image](https://user-images.githubusercontent.com/98153604/202294393-85befe9f-7978-4a59-82ad-be5b18cb7bf5.png)





