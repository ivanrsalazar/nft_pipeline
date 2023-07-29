# NFT Sales Data Collection and Analysis



### Overview
The main objective of this pipeline is to streamline the collection and processing of blockchain and sale events data from multiple NFT collections. By ingesting data from various sources, the pipeline ensures a seamless flow of information related to NFT sales events. The cleaned and transformed data is then stored in a centralized data warehouse, following the NFT Sale Transaction data model. This centralized repository provides a comprehensive and structured view of NFT transactions, facilitating in-depth analysis and valuable insights into the NFT market.


### Setup
-  Two remote EC2 instances responsible for establishing a long-lasting WebSocket connection to [OpenSea Stream API](https://docs.opensea.io/reference/stream-api-overview)
- The instances continuously capture event data from various NFT collections and write it to a local file
- The data is batched hourly, where the application writes to a new file as soon as an event from a new hour is received
- The collected data is uploaded to S3 via an hourly cron job 
- Apache Airflow is run on a local machine, and is used to transform and load the past hours sales event data to an RDS PostgerSQL data warehouse











####