# Data Pipeline Clientlib

[![Build Status](https://travis-ci.org/Yelp/data_pipeline.svg?branch=master)](https://travis-ci.org/Yelp/data_pipeline)

What is it?
-----------
Data Pipeline Clientlib provides an interface to tail and publish to data pipeline topics.

[Read More](https://engineeringblog.yelp.com/2016/07/billions-of-messages-a-day-yelps-real-time-data-pipeline.html)


How to download
---------------
```
git clone git@github.com:Yelp/data_pipeline.git
```


Tests
-----
Running unit tests
```
make -f Makefile-opensource test
```


Configuration
-------------
Include the `data_pipeline` namespace in your `module_env_config` of `config.yaml`
and configure following values for `kafka_ip`, `zk_ip` and `schematizer_ip`

```
module_env_config:
	...
    - namespace: data_pipeline
      config:
        kafka_broker_list:
            - <kafka_ip>:9092
        kafka_zookeeper: <zk_ip>:2181
        schematizer_host_and_port: <schematizer_ip>:8888
    ...
```


Usage
-----
Registering a simple schema with the Schematizer service.
```
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
test_avro_schema_json = {
    "type": "record",
    "namespace": "test_namespace",
    "source": "test_source",
    "name": "test_name",
    "doc": "test_doc",
    "fields": [
        {"type": "string", "doc": "test_doc1", "name": "key1"},
        {"type": "string", "doc": "test_doc2", "name": "key2"}
    ]
}
schema_info = get_schematizer().register_schema_from_schema_json(
    namespace="test_namespace",
    source="test_source",
    schema_json=test_avro_schema_json,
    source_owner_email="test@test.com",
    contains_pii=False
)
```

Creating a simple Data Pipeline Message from payload data.
```
from data_pipeline.message import Message
message = Message(
    schema_id = schema_info.schema_id,
    payload_data = {
        'key1': 'value1',
        'key2': 'value2'
    }
)
```

Starting a Producer and publishing messages with it::
```
from data_pipeline.producer import Producer
with Producer() as producer:
    producer.publish(message)
```

Starting a Consumer with name `my_consumer` that listens for
messages in all topics within the `test_namespace` and `test_source`.
In this example, the consumer consumes a single message, processes it, and
commits the offset.
```
from data_pipeline.consumer import Consumer
from data_pipeline.consumer_source import TopicInSource
consumer_source = TopicInSource("test_namespace", "test_source")
with Consumer(
    consumer_name='my_consumer',
    team_name='bam',
    expected_frequency_seconds=12345,
    consumer_source=consumer_source
) as consumer:
    while True:
        message = consumer.get_message()
        if message is not None:
            ... do stuff with message ...
            consumer.commit_message(message)
```


Disclaimer
-------
We're still in the process of setting up this package as a stand-alone. There may be additional work required to run Producers/Consumers and integrate with other applications.


License
-------
Data Pipeline Clientlib is licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0


Contributing
------------
Everyone is encouraged to contribute to Data Pipeline Clientlib by forking the Github repository and making a pull request or opening an issue.
