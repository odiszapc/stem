# Stem Object Storage

Stem is a distributed object storage based on top of Netty, Grizzly and Zookeeper.

Stem is designed as a cheap solution for cloud providers and can be deployed on a commodity hardware, it doesn't need any vendor specific staff like RAID or SAN. It uses plain formatted disks to store binary data.

With Stem you can store tens of terabytes of data on each machine in cluster.

#### Features
- Linux/Windows compatible (JVM)
- Heterogeneous cluster model (binary objects are separated from meta data)
- Low cost — store up to 200TB of data per a single machine
- Zero-overhead compaction process (bye bye, TCS and LCS)
- Replication (x3 by default)
- Automated cluster re-balancing logic
- Tolerance for disk fragmentation by pre-allocating disk space
- Sequential writes
- Adjustable consistency on reads
- Coordinate and monitoring of data movement with Zookeeper
- Uses Cassandra as a  meta data registry
- Cluster Management UI
- Hierarchical cluster topology
- Data distribution is controlled with CRUSH algorithm
- REST API (PUT, GET, DELETE)

#### Use cases
Stem can act as a base for a wide variety of services:
- File storage backend
- Storage for a huge amount of small blocks of data
- Core engine for image hosting

Stem Object Storage enables you to install as many HDDs to a cluster node as you want. When meta data are in a separated place it's easy — you are not afraid to exceed the RAM.

#### Resources
Web site: http://stemstorage.org

Issue tracker: http://tracker.stemstorage.org (JIRA)

#### Authors
Alexey Plotnik (odiszapc@gmail.com, http://twitter.com/odiszapc) I do it just because I like it.

#### License
Apache 2.0
