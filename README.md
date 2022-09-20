[![license](https://img.shields.io/github/license/RedisBloom/JRedisBloom.svg)](https://github.com/RedisBloom/JRedisBloom)
[![GitHub issues](https://img.shields.io/github/release/RedisBloom/JRedisBloom.svg)](https://github.com/RedisBloom/JRedisBloom/releases/latest)
[![CircleCI](https://circleci.com/gh/RedisBloom/JRedisBloom/tree/master.svg?style=svg)](https://circleci.com/gh/RedisBloom/JRedisBloom/tree/master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.redislabs/jrebloom/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.redislabs/jrebloom)
[![Javadocs](https://www.javadoc.io/badge/com.redislabs/jrebloom.svg)](https://www.javadoc.io/doc/com.redislabs/jrebloom)
[![Codecov](https://codecov.io/gh/RedisBloom/JRedisBloom/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisBloom/JRedisBloom)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/RedisBloom/JRedisBloom.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/RedisBloom/JRedisBloom/context:java)
[![Known Vulnerabilities](https://snyk.io/test/github/RedisBloom/JRedisBloom/badge.svg?targetFile=pom.xml)](https://snyk.io/test/github/RedisBloom/JRedisBloom?targetFile=pom.xml)

# JRedisBloom
[![Forum](https://img.shields.io/badge/Forum-RedisBloom-blue)](https://forum.redislabs.com/c/modules/redisbloom)
[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/wXhwjCQ)

A Java Client Library for [RedisBloom](https://redisbloom.io)

## Deprecation notice

As of [Jedis](https://github.com/redis/jedis) version 4.2.0, this library is deprecated. Its features have been merged into Jedis. Please either install it from [maven](https://mvnrepository.com/artifact/redis.clients/jedis) or [the repo](https://github.com/redis/jedis)

## Overview

This project contains a Java library abstracting the API of the RedisBloom Redis module, that implements a high
performance bloom filter with an easy-to-use API
 
See [http://redisbloom.io](http://redisbloom.io) for installation instructions of the module.


### Official Releases

```xml
  <dependencies>
    <dependency>
      <groupId>com.redislabs</groupId>
      <artifactId>jrebloom</artifactId>
      <version>2.1.0</version>
    </dependency>
  </dependencies>
```

### Snapshots

```xml
  <repositories>
    <repository>
      <id>snapshots-repo</id>
      <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    </repository>
  </repositories>
```

and
```xml
  <dependencies>
    <dependency>
      <groupId>com.redislabs</groupId>
      <artifactId>jrebloom</artifactId>
      <version>2.2.0-SNAPSHOT</version>
    </dependency>
  </dependencies>
```


## Usage example

Initializing the client:

```java
import io.rebloom.client.Client

Client client = new Client("localhost", 6379);
```

Adding items to a bloom filter (created using default settings):

```java
client.add("simpleBloom", "Mark");
// Does "Mark" now exist?
client.exists("simpleBloom", "Mark"); // true
client.exists("simpleBloom", "Farnsworth"); // False
```


Use multi-methods to add/check multiple items at once:

```java
client.addMulti("simpleBloom", "foo", "bar", "baz", "bat", "bag");

// Check if they exist:
boolean[] rv = client.existsMulti("simpleBloom", "foo", "bar", "baz", "bat", "mark", "nonexist");
```

Reserve a customized bloom filter:

```java
client.createFilter("specialBloom", 10000, 0.0001);
client.add("specialBloom", "foo");

```

Use cluster client to call redis cluster
Initializing the cluster client:
```java
Set<HostAndPort> jedisClusterNodes = new HashSet<>();
jedisClusterNodes.add(new HostAndPort("localhost", 7000));
ClusterClient cclient = new ClusterClient(jedisClusterNodes);
```

Adding items to a bloom filter (created using default settings):

```java
cclient.add("simpleBloom", "Mark");
// Does "Mark" now exist?
cclient.exists("simpleBloom", "Mark"); // true
cclient.exists("simpleBloom", "Farnsworth"); // False
```

all method of ClusterClient is same to Client.
