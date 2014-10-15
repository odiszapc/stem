# Stem Object Storage

STEM is a distributed object storage based on top of Netty, Grizzly and Zookeeper.

STEM is designed as a cheap solution for cloud storage hosting providers and can be easily be deployed on commodity hardware. STEM does not need any vendor specific staff like RAID or SAN, plain formatted disks are used to store binary data.

With STEM you can persist up to 100-200TB+ of binary data on each machine in cluster.

#### Features
- Linux/Windows compatible (HotSpot JVM)
- Heterogeneous design of cluster (binary dataset is separated from meta data)
- Low cost — store up to 200TB of data on storage node
- Stray forward compaction with no overhead (it takes few hours to free up space after deletion of data on a 3TB disk)
- Replication (x3 by default)
- Automated cluster re-balancing logic — turn on new node and that's all
- Tolerance to disk fragmentation (pre-allocation of disk space)
- True sequential writes
- Adjustable consistency on reads
- Coordination and monitoring of data movement with Zookeeper
- Uses Cassandra 2.0 as registry of meta data
- Cluster management user interface
- Hierarchical cluster topology
- Using CRUSH algorithm to control data distribution
- REST API (PUT, GET, DELETE)

#### Use cases
STEM can be a key component in the following kind of services:
- File storage backend
- Storage for a large amount of small blocks of data
- Core engine for image hosting
- Video hosting back-end

Stem Object Storage will allow you to install as many disks to storage node as you want. Indexes are located in a separate cluster; you don't need to worry about running out of RAM — meta- and binary- clusters can be scaled out separately.

#### Resources
Web site: http://stemstorage.org

Issue tracker: http://tracker.stemstorage.org (JIRA)

#### Authors
Alexey Plotnik (odiszapc@gmail.com, http://twitter.com/odiszapc)

#### License
Apache 2.0
