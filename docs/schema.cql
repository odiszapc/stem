CREATE KEYSPACE stem WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

USE stem;

CREATE TABLE blobs_meta (
  blob blob,
  disk uuid,
  data blob,
  PRIMARY KEY (blob, disk)
)
WITH COMPACT STORAGE AND
compaction = {
    'class': 'LeveledCompactionStrategy',
    'sstable_size_in_mb': 256,
    'tombstone_compaction_interval': 86400,
    'tombstone_threshold': 0.2
};