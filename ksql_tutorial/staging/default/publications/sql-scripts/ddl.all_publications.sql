CREATE TABLE IF NOT EXISTS all_publications (
  bookid BIGINT,
  author STRING,
  title STRING,
  PRIMARY KEY (bookid) NOT ENFORCED
) DISTRIBUTED BY HASH(bookid) INTO 1 BUCKETS WITH (
    'changelog.mode'= 'append',
    'VALUE.format'= 'json-registry',
    'kafka.retention.time'= '0','kafka.producer.compression.type  '= 'snappy','scan.bounded.mode  '= 'unbounded','scan.startup.mode  '= 'earliest-offset','VALUE.fields - include  '= 'all'");