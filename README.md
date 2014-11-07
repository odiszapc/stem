# STEM Object Storage

STEM is a distributed storage cluster for immutable binary objects of arbitary sizes. STEM is based on top of Netty, Grizzly and Zookeeper. STEM has its own data storage format.

STEM is designed as a cheap solution for cloud hosting providers and can easily be deployed on commodity hardware. STEM does not need any vendor specific staff like RAID or SAN, it uses plain formatted disks to store binary data.

With STEM each machine in cluster can store up to 100-200TB of data.

#### Features
- Linear scalability
- High availability (x3 replication by default)
- Heterogeneous design of cluster (binary objects are separated from meta data)
- Linux/Windows compatible (HotSpot JVM)
- Low cost — store up to 200TB of data on storage node
- Stray forward compaction with no overhead (it takes few hours to free up space after deletion of data on a 3TB disk)
- Automated cluster re-balancing logic — join a new node to cluster and power on it
- Data repairing process has no overhead while calculating deltas of data
- Tolerance to disk fragmentation (pre-allocation of disk space)
- True sequential writes
- Adjustable consistency on reads
- Coordination and monitoring of data movement with Zookeeper
- Uses Cassandra 2.0 as meta-data registry
- Cluster management user interface
- Hierarchical cluster topology
- Data distribution logic is based on CRUSH algorithm to compute data flow
- REST API (PUT, GET, DELETE)

#### Use cases
Linear scalability, high availability and zer-overhead compaction and repair make STEM perfect platform for the following production cases:
- File storage backend
- Storage for a large amount of small blocks of data
- Core engine for image hosting
- Video hosting back-end

STEM Object Storage will allow you to install as many disks to storage node as you want. Indexes are located in a separate cluster; you don't need to worry about running out of RAM — meta- and binary- clusters can be scaled out separately.

#### Resources
Project home: http://stemstorage.org

JIRA: http://tracker.stemstorage.org

#### Authors
Alexey Plotnik (odiszapc@gmail.com, http://twitter.com/odiszapc)

#### License
Apache 2.0
