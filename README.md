# NFT Sales Data Collection and Analysis



### Overview
The main objective of this pipeline is to streamline the collection and processing of blockchain and sale events data from multiple NFT collections. By ingesting data from various sources, the pipeline ensures a seamless flow of information related to NFT sales events. The cleaned and transformed data is then stored in a centralized data warehouse, following the NFT Sale Transaction data model. The grain of the warehouse is one sale event per transaction. This centralized repository provides a comprehensive and structured view of NFT transactions, facilitating in-depth analysis and valuable insights into the NFT market.


### Setup
-  Two remote EC2 instances responsible for establishing a long-lasting WebSocket connection to [OpenSea Stream API](https://docs.opensea.io/reference/stream-api-overview)
- The instances continuously capture event data from various NFT collections and write it to a local file
- The data is batched hourly, where the application writes to a new file as soon as an event from a new hour is received
- The collected data is uploaded to S3 via an hourly cron job 
- Apache Airflow is run on a local machine, and is used to download, transform, and load the past hours sales event data to an RDS PostgreSQL data warehouse

### Data Sources

- [Etherscan.io](https://etherscan.io/apis)
    - Normal Transaction data
    - Internal Transaction data
    - ERC-20 Transaction data
    - Block data

<br>

- [Alchemy.com](https://alchemy.com)
  - Historical Sales data
  - Historical Block data

<br>

- [Opensea.io](https://docs.opensea.io/reference/api-overview)
  - Current NFT events data

### File Structure
#### api_tools.py
This file serves as a central repository housing all functions used to make API calls to the Etherscan and Alchemy platforms. These functions facilitate seamless interactions with the Ethereum blockchain and provide access to essential blockchain data and information.

#### NftTransction.py
This file serves as a blueprint for NftTransaction objects, which are designed to encapsulate and manage data related to Non-Fungible Token (NFT) transactions. NftTransaction objects act as containers for storing crucial information about individual NFT transactions, providing a structured and organized representation of the data.

- attributes
    - events
        - list
        - holds JSON event data objects
    - parties
        - dictionary
        - key = (collection,token_id) 
        - value = (seller address,buyer address)
    - block_number
        - integer
        - Ethereum confirmation block
    - time_stamp
        - string
        - time stamp when transaction block was confirmed
    - market_place
        - string
        - platform where the transaction originated
    - token_ids
        - list
        - holds token_ids (int) for events associated within transaction
    - collection
        - string
        - name of the NFT collection
    - transaction_hash
        - string
        - output hash of the Ethereum blockchain transaction
    - sales_prices
        - dictionary
        - key = (collection, token_id)
        - value = sale price of the NFT
    - buyer_qty
        - dictionary
        - key = buyer_address
        - value = quantity of NFTs purchased by address within this transaction
    - init_address
        - string
        - address which initiated the Normal transaction
    - method_call
        - string
        - the method called to initiate the Normal transaction
    - nonce
        - string
        - incremental number associated with ethereum block and transaction


- methods
    - printTransaction(self)
        - output all the important transaction data
        - used for logging and debugging

        <br>

    - printSales(self)
        - output all sales data
        - used for logging and debugging

    <br>

    - get_queries(self, nftDF)
        - inputs a pandas DF holding the nft_id primary keys
        - outputs insert_queries, insert_tuples
        - insert_queries holds a list of SQL insert queries
        - insert_tuples holds a list of unique sale fact tuples


#### update_db.py

This file represents an Airflow Directed Acyclic Graph (DAG) responsible for managing incremental data loads into the data warehouse.

- functions
    - create_inserts
        - This function downloads the past hours' event data from S3 and performs transformations, then writes valid sales to a SQL file
    - update_db
        - This function is triggered once create_inserts completes, and writes all the insert queries to my remote data warehouse in the cloud

#### past_sales.py

This file serves as a dedicated manager for handling the historical load of past Non-Fungible Token (NFT) sales data. Its primary purpose is to automate the process of ingesting and processing historical sales data, allowing users to analyze and leverage historical NFT sales information efficiently.


#### collection_app.py

This establishes the long lasting connection to OpenSea's
Stream API

Note that this file is meant to be a general template for subcribing to
one collection, if you want to subscribe to more than one collection, 
you need to find the collection slug and update the file

Also, you probably want to be running these files are more than one machine to increase availabilty and redundancy. I would suggest a minimum of two machines, preferably on the cloud.

#### 


## Required
- asyncio
- websockets
- ssl
- certifi
- logging
- requests
- json
- datetime
- time
- pycryptodome
- os
- airflow
- boto3
- psycopg2
- pandas
- botocore

####