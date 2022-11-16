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

Create Api keys, Kafka API keys are required to interact with Kafka cluster in Confluent Cloud

![image](https://user-images.githubusercontent.com/98153604/202294772-9e69b944-4e71-42d2-a1c4-d7b4f5d079e3.png)

Select Global access:

![image](https://user-images.githubusercontent.com/98153604/202294891-40ed5a45-164f-4c82-ae32-6f59aa5d11ae.png)

![image](https://user-images.githubusercontent.com/98153604/202294938-47f3a993-9266-49ca-8d93-9846cf244ccf.png)

## 2. Create 2 new Kafka topics for 2 streaming data flow source

![image](https://user-images.githubusercontent.com/98153604/202295764-6c57ad87-84df-4edb-916a-3ee6191b8b18.png)

![image](https://user-images.githubusercontent.com/98153604/202295823-67f80dff-0b61-4daf-b837-462b97523d33.png)

![image](https://user-images.githubusercontent.com/98153604/202295894-dc9ec0d6-66a1-4ee9-85e6-ddd9a4efb0a7.png)

## 3. Connect data producers with Topics

Confluent platform provide an connectors for easily connecting Kafka topics, one is called Datagen Source, which is help to generate some fake continues data flow and can connect to specific Kafka topics for simulate real situations

![image](https://user-images.githubusercontent.com/98153604/202296560-dede0f6f-8584-4b66-9965-8fcae8102846.png)

enter the API key which create before

![image](https://user-images.githubusercontent.com/98153604/202296609-5a5e2435-239d-4c9f-be59-cdd3368cdf98.png)

Select event data format and type

![image](https://user-images.githubusercontent.com/98153604/202296786-38057313-cd30-4518-b9f4-7e435222dfd6.png)


![image](https://user-images.githubusercontent.com/98153604/202296970-fde6aec3-9e5a-43c0-8360-43e0d56c143c.png)

![image](https://user-images.githubusercontent.com/98153604/202297147-e4a1ad45-8fd9-478a-88f1-9792d730e661.png)

![image](https://user-images.githubusercontent.com/98153604/202297241-f6681c50-7d95-49c2-bfba-629eabf63a69.png)

![image](https://user-images.githubusercontent.com/98153604/202297254-0b27b2af-1ed2-4fb8-a9ac-de380d01e328.png)

I create 2 fake data flow, one is for customer click stream data, one is for user look-up data, user are also growing continues

![image](https://user-images.githubusercontent.com/98153604/202297525-2b890a00-f5bf-4995-95f7-13678e4bae81.png)

![image](https://user-images.githubusercontent.com/98153604/202297580-3856c48c-b069-4f96-b131-cede7aa2dcf3.png)















