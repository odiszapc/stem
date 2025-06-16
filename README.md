# STEM Object Storage

**STEM** is a distributed object storage system designed to store immutable binary data. It is built on top of Netty, Grizzly, ZooKeeper, and Cassandra (used as a metadata store). STEM implements its own storage format using pre-allocated blob containers called *fat files*, which are uniform in size.

STEM utilizes a data distribution strategy based on virtual partitioning, with a default implementation of the CRUSH algorithm.

It is designed as a cost-effective solution for cloud hosting providers and can be deployed on commodity hardware. STEM requires no vendor-specific components such as RAID or SAN; it uses plain formatted disks for binary data storage.

Each machine in the cluster can handle up to 100–200 TB of data.

According to the CAP theorem, STEM is an **AP** (Availability and Partition tolerance) system.

STEM was inspired by the [Twitter Blobstore storage model](https://blog.twitter.com/engineering/en_us/a/2012/blobstore-twitter-s-in-house-photo-storage-system.html).

---

### Features

- Linear scalability
- No single point of failure
- High availability (3x replication by default)
- Heterogeneous cluster design (binary objects are decoupled from metadata)
- Compatible with Linux and Windows (HotSpot JVM)
- Low-cost storage — up to 200 TB per storage node
- Straightforward compaction with minimal overhead (e.g., a 3 TB disk can be compacted within hours)
- Automated cluster rebalancing — just rack and power on
- Efficient data recovery via differential set calculation
- Tolerant of disk fragmentation through pre-allocation of space
- True sequential writes to maximize spindle disk performance
- Tunable read consistency
- ZooKeeper-based coordination and monitoring of data movement
- Cassandra 2.0 used as metadata registry
- Web-based cluster management interface
- Hierarchical cluster topology
- Data distribution logic based on the CRUSH algorithm
- REST API (supports PUT, GET, DELETE)

---

### Use Cases

With linear scalability, high availability, and zero-overhead compaction and repair, STEM is ideal for:

- File storage backends (e.g., video hosting)
- Storage of a large volume of small binary objects
- Image hosting core engines
- Video hosting backends

STEM allows you to attach an arbitrary number of disks to a storage node. Indexes are stored in a separate metadata cluster, so memory limitations on storage nodes are not a concern. Binary and metadata clusters can be scaled independently.

---

### Development

#### IntelliJ IDEA

- If you're using IntelliJ, install the [Lombok plugin](https://plugins.jetbrains.com/plugin/6317-lombok) for proper annotation support.
- Import the project as a Maven project.

#### Eclipse and Variants (STS, JBoss Developer Studio)

- Run `lombok.jar` as a Java application to install the plugin.
- Import the project as a Maven project.
- You may need to suppress some m2e plugin warnings.

> Why Eclipse? Use `vi`!

---

### Resources

- Project homepage: [http://stemstorage.org](http://stemstorage.org)
- JIRA issue tracker: [http://tracker.stemstorage.org](http://tracker.stemstorage.org)

---

### Author

- Alexey Plotnik ([odiszapc@gmail.com](mailto:odiszapc@gmail.com), [@odiszapc](http://twitter.com/odiszapc))

---

### Contributors

- Alexey Plotnik  
- Dmitry Kolesov  
- Ivan Sobolev  
- Lucas Allan Amorim  

---

### License

Apache License 2.0
