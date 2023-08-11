import csv
import os
import requests
import json
import datetime
import time
from requests.adapters import HTTPAdapter, Retry
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import TimestampType
from pyspark.sql.types import LongType
from pyspark.sql import Row
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.functions import col

alc_key = os.environ.get("alc_key")


def get_blockdata(block_number):
    '''
    Description:
        returns a dictionary with data about the ethereum block
        keys: timestamp, difficulty, totalDifficulty, gasUsed, gasLimit

        difficulty : scalar value that represents how hard is was to validate the block
        totalDifficulty : summation of the difficulties of all previous blocks plus the current
        gasUsed: summation of gas fees in the block
        gasLimit: The limit of gas that can be included in a block

    Parameter:
        block_number (integer)

    Returns:
        data (dictionary)
        keys: timestamp, difficulty, totalDifficulty, gasUsed, gasLimit

    '''
    url = f"https://eth-mainnet.g.alchemy.com/v2/{alc_key}"

    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [str(hex(block_number)), True]
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    data = {}
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    response = None
    while(True):
        try:
            response = session.post(url, json=payload, headers=headers)
        except:
            pass
        if response != None and response.status_code == 200:
            break
        else:
            time.sleep(1)
    
    json_response = json.loads(response.text)


    result = json_response['result']    

    unix_time = result['timestamp']
    hex_difficulty = result['difficulty']
    hex_total_difficulty = result['totalDifficulty']
    hex_gas_used = result['gasUsed']
    hex_gas_limit = result['gasLimit']

    unix_time = int(unix_time,16)
    timestamp = datetime.datetime.utcfromtimestamp(unix_time)
    data['blocknumber'] = block_number
    data['timestamp'] = timestamp
    data['difficulty'] = int(hex_difficulty,16)
    data['total_difficulty'] = int(hex_total_difficulty,16)
    data['gas_used'] = int(hex_gas_used,16)
    data['gas_limit'] = int(hex_gas_limit,16)

    return data
    


spark = SparkSession.builder.appName("block_app").getOrCreate()

schema = StructType([
    StructField('block_number', LongType()),
    StructField('timestamp', TimestampType()),
    StructField('difficulty', LongType()),
    StructField('total_difficulty', LongType()),
    StructField('gas_used', LongType()),
    StructField('gas_limit', LongType())
])

sch_block = StructType([
    StructField('block_number', LongType())
])


block_udf = udf(get_blockdata,schema)


block_numbers = []
for block_number in range(11565019, 17877041, 1):
    block_numbers.append(Row(block_number))


block_df = (
        spark
        .createDataFrame(block_numbers, sch_block)
        .repartition(6)
        .withColumn ('pid', spark_partition_id())
    )


block_df_data = block_df.withColumn('block_col', block_udf(col('block_number')))

block_df_data.write.mode('overwrite').parquet('gs://databeans/blocks')

spark.stop()
