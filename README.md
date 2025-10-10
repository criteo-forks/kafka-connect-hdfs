# Kafka Connect HDFS Connector

kafka-connect-hdfs is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect)
for copying data between Kafka and Hadoop HDFS.

Documentation for this connector can be found [here](http://docs.confluent.io/current/connect/connect-hdfs/docs/index.html).

# Criteo fork changes

- Disable all hive related test raising a  `NoClassDefFound Could not initialize class org.apache.hadoop.hive.ql.exec.Utilities`. Related issue (https://github.com/criteo-forks/kafka-connect-hdfs/issues/1). To be fixed if we plan to use hive module (not the case currently).
- Apply unmerged PR https://github.com/confluentinc/kafka-connect-hdfs/pull/684 to solve the rotate Interval that doesn't work for low volume or irregular traffic

# Development

To build a development version you'll need a recent version of Kafka as well as a set of upstream Confluent projects, which you'll have to build from their appropriate snapshot branch. See the [FAQ](https://github.com/confluentinc/kafka-connect-hdfs/wiki/FAQ) for guidance on this process.

You can build kafka-connect-hdfs with Maven using the standard lifecycle phases.

# FAQ

Refer frequently asked questions on Kafka Connect HDFS here -
https://github.com/confluentinc/kafka-connect-hdfs/wiki/FAQ

# Contribute

- Source Code: https://github.com/confluentinc/kafka-connect-hdfs
- Issue Tracker: https://github.com/confluentinc/kafka-connect-hdfs/issues
- Learn how to work with the connector's source code by reading our [Development and Contribution guidelines](CONTRIBUTING.md).

# License

This project is licensed under the [Confluent Community License](LICENSE).
