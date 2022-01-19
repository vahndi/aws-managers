class AthenaSerDes(object):
    """
    https://docs.aws.amazon.com/athena/latest/ug/supported-serdes.html
    """
    Avro = 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
    CloudTrail = 'com.amazon.emr.hive.serde.CloudTrailSerde'
    Grok = 'com.amazonaws.glue.serde.GrokSerDe'
    HiveJSON = 'org.apache.hive.hcatalog.data.JsonSerDe'
    LazySimple = 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    OpenCSV = 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    OpenXJSON = 'org.openx.data.jsonserde.JsonSerDe'
    ORC = 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    Parquet = 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    Regex = 'org.apache.hadoop.hive.serde2.RegexSerDe'
