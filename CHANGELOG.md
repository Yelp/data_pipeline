This changelog only guarantees backward incompatible changes starting from version 1.0.0 will be listed.
# v3.0.0 (2019-11-04)
## backward incompatible changes
- In Consumer class, reset_topics(topic_to_partition_offset_map) and commit_offsets(topic_to_partition_offset_map) functions take in physical streams to partition offset map.
  * If the consumer is using schematizer based discovery, you can match logical stream with physical stream by calling [get_latest_physical_stream_by_logical_stream_name_source_region](http://swagger_ui.paasta-norcal-devc.yelp/?service=schematizer#/logical_streams/get_latest_physical_stream_by_logical_stream_name_source_region) API exposed by schematizer.
  * If the consumer is not using schematizer based discovery,
    + If the cluster type is 'datapipe', physical stream will share the same name as logical stream.
    + If the cluster type is 'scribe', you will need to use yelp_kafka.discovery.get_region_logs_stream to fetch the physical stream, see more details [here](http://servicedocs.yelpcorp.com/docs/yelp_kafka/discovery.html#yelp_kafka.discovery.get_region_logs_stream).
# v1.0.0 (2017-09-07)
## backward incompatible changes
- All classes from data_pipeline/schematizer_clientlib/ were moved to schematizer/ in schematizer package.
    To use you will need to add schematizer python package as dependency and change your import, like:
        -from data_pipeline.schematizer_clientlib.models.refresh import RefreshStatus
        +from schematizer.models.refresh import RefreshStatus

- New SchematizerClient api in data_pipeline/schematizer.py
    import example:
        -from data_pipeline.schematizer_clientlib.schematizer import SchematizerClient
        +from schematizer.schematizer import SchematizerClient
    Method signature:
        -SchematizerClient(datapipe_config)
        +SchematizerClient(max_connection_retry=5, schematizer_host_and_port=None)

- Function get_schematizer() moved from data_pipeline/schematizer_clientlib/schematizer.py to data_pipeline/schematizer.py
