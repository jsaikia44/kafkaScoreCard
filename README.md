
Apache Kafka:
Apache Kafka is a distributed publish-subscribe messaging system and handle a high volume of data and enables you to pass messages from one end-point to another. Kafka is suitable for both offline and online message consumption. Here it is used for only offline cases.

Kafka is a unified platform for handling all the real-time data feeds. Kafka supports low latency message delivery and gives guarantee for fault tolerance in the presence of machine failures. It has the ability to handle a large number of diverse consumers. Kafka is very fast, performs 2 million writes/sec. Kafka persists all data to the disk, which essentially means that all the writes go to the page cache of the OS (RAM). This makes it very efficient to transfer data from page cache to a network socket.

Objective:
Handle data streams and process the data in real time.

Description:
Every producer is writing the commentary (message) to Kafka server. Each commentary line is prefixed with match id and innings id. 
The match id serve as topics. The messages are not ordered in any specified manner. The commentary will be interleaved in nature.
There are 45 producers one corresponding to each match id and 45 consumers one corresponding to each match id. Consumers subscribe to 
the commentary based on the topic.
