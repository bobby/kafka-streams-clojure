# kafka-streams-clojure

[Clojure transducers](https://clojure.org/reference/transducers)
interface to
[Kafka Streams](https://kafka.apache.org/documentation/streams).  This
combo provides the best of both worlds for building streaming
applications on Kafka with Clojure:

* Simple, declarative, idiomatic, composable, testable stream
  transformation logic, via transducers
* Easy, battle-hardened distributed system topology specification,
  cluster partition rebalancing, local state management, etc. via Kafka
  Streams

## Status

**THIS LIBRARY IS CURRENTLY ALPHA STATUS, AND IS NOT FIT FOR PRODUCTION USE!**

This notice will be removed when I believe the API is stable and the
library has performed well under heavy loads in real-world use.

### Features & Roadmap

Currently, this library supports:

* Hooking a transducer into a `KStream` processing pipeline.

In the future, I plan for this library to support:

* Helper transducers for stateful computations like joins, windowed
  aggregates, etc. to mirror the functionality of the `KStream` API,
  but which can be composed with purely functional steps
* An appropriate level of integration into both the low-level
  `Processor` API and the `KTable` APIs.

## Installation

**Note: Due to its alpha status, this library is not configured for
CI/CD, and no JARs have been pushed to a public repository.  You'll
have to install (as per instructions below) into your local Maven repo
before the following instructions will work**

Include the library JAR in your Boot/Leiningen dependencies:

``` clojure
[kafka-streams-clojure "0.1.0-SNAPSHOT"]
```

### Kafka Streams Dependency

Kafka Streams is included as a `provided` dependency, meaning your
application will need to include the
[Kafka Streams JAR](https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams)
as a dependency as well as this library.

## Usage

Transducers provide a more Clojure-idiomatic way to transform
streaming key value pairs than `KStream`'s Java 8 Streams-like API.
The key function is `kafka-streams-clojure.api/transduce-kstream`,
which makes the given `KStream` a transducible context by applying the
given transducer as a `Transformer`.  The step function is invoked
with the `ProcessorContext` and a 2-tuple of `[key value]` for each
record, so the transducer should be shaped accordingly.

This library also provides a number of stateful transducers over Kafka
Streams' Stores API for doing joins, windowed aggregates, etc.  The
goal of this library is to maintain feature parity with the high-level
`KStream`, `KTable`, etc. APIs, as well as (eventually) to enable
transducer usage in the low-level `Processor` API.

``` clojure
// Start Kafka Cluster running locally

(require '[kafka-streams-clojure.api :as api])
(import '[org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
        '[org.apache.kafka.streams StreamsConfig KafkaStreams]
        '[org.apache.kafka.streams.kstream KStreamBuilder])

(def xform (comp (filter (fn [[k v]] (string? v)))
                 (map (fn [[k v]] [v k]))
                 (filter (fn [[k v]] (= "foo" v)))))
(def builder (KStreamBuilder.))
(def kstream (-> builder
                 (.stream (into-array String ["tset"]))
                 (api/transduce-kstream xform)
                 (.to "test")))

(def kafka-streams
  (KafkaStreams. builder
                 (StreamsConfig. {StreamsConfig/APPLICATION_ID_CONFIG    "test-app-id"
                                  StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9092"
                                  StreamsConfig/KEY_SERDE_CLASS_CONFIG   org.apache.kafka.common.serialization.Serdes$StringSerde
                                  StreamsConfig/VALUE_SERDE_CLASS_CONFIG org.apache.kafka.common.serialization.Serdes$StringSerde})))
(.start kafka-streams)

(def producer (KafkaProducer. {"bootstrap.servers" "localhost:9092"
                               "acks"              "all"
                               "retries"           "0"
                               "key.serializer"    "org.apache.kafka.common.serialization.StringSerializer"
                               "value.serializer"  "org.apache.kafka.common.serialization.StringSerializer"}))

@(.send producer (ProducerRecord. "tset" "foo" "bar"))
// Observe message come across topic "test" via kafka-console-consumer

@(.send producer (ProducerRecord. "tset" "baz" "quux"))
// Observe message does not come across topic "test" via kafka-console-consumer

(.close producer)
(.close kafka-streams)
```

## Dev, Build, Test

This project uses [Leiningen](https://leiningen.org/) for dev, test,
and build workflow.

### Run Tests

The test include an embedded, single-node Kafka/ZooKeeper cluster that
runs on demand.

``` bash
lein test
```

### Run REPL

To run via the REPL, you'll need to fire up a Kafka Cluster.

``` bash
lein repl
```

### Build and Push JAR

``` bash
lein jar
lein deploy
```

## License

```
Copyright 2017 Bobby Calderwood

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
