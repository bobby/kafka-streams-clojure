(defproject kafka-streams-clojure "0.1.0-SNAPSHOT"
  :description "Kafka Streams integration with Clojure transducers."
  :url "https://github.com/bobby/kafka-streams-clojure"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.apache.kafka/kafka-streams "0.10.2.1" :scope "provided"]]
  :profiles {:dev {:resource-paths ["dev"]
                   :dependencies [[org.apache.kafka/kafka_2.11 "0.10.2.1"]
                                  [ch.qos.logback/logback-classic "1.1.7"]
                                  [org.slf4j/jcl-over-slf4j "1.7.21"]
                                  [org.slf4j/log4j-over-slf4j "1.7.21"]
                                  [org.apache.logging.log4j/log4j-to-slf4j "2.7"]
                                  [org.slf4j/jul-to-slf4j "1.7.21"]]
                   :exclusions   [commons-logging
                                  log4j
                                  org.apache.logging.log4j/log4j
                                  org.slf4j/simple
                                  org.slf4j/slf4j-jcl
                                  org.slf4j/slf4j-nop
                                  org.slf4j/slf4j-log4j12
                                  org.slf4j/slf4j-log4j13
                                  org.slf4j/osgi-over-slf4j]}})
