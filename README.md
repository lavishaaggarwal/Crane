# Crane
We build a fully distributed system which is resistant to multiple simultaneouus failures through the following steps:
1. Distributed Log Querier system which performs a distributed grep over log files present on multiple virtual machines. The grep can be requested by any client on any virtual machine
2. Distributed Membership system which implements a SWIM based infection style protocol. Each member maintains the membership information of all nodes in the group. Each node keeps track of its predecessors and successors, all noded arranged in a virtual ring, via PING-ACK messages. Node additions are processed through an introducer node which can not fail.
3. Distributed File System in which a fully distributed file system with 4 replicas of each file is maintained. This can withstand 3 simultaneous failures. File operations like put, get, modify, ls, find different versions, delete, etc. are supported and are totally ordered through a leader. To deal with leader failure a ring based leader election is implemented.
4. Crane - A Distributed Stream processing system based on Storm and faster than Spark is built. This stream processing system operated on streams of tuples flowing between spout, bolts and sink with support for 3 primary operations - Filter, Join and Transform.

To run Crane, clone this repository and follow the steps in the README present inside the  Distributed Stream Processing directory.
