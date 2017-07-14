;; from https://github.com/Mayvenn/embedded-kafka/blob/c0092b3e48a009238a645ee79d65e0f9de27999b/src/embedded_kafka/core.clj

(ns kafka-streams-clojure.embedded-kafka
  (:import
   [kafka.server KafkaConfig KafkaServerStartable]
   [org.apache.kafka.clients.producer Producer KafkaProducer]
   [org.apache.kafka.clients.consumer Consumer KafkaConsumer]
   [java.net InetSocketAddress]
   [org.apache.zookeeper.server ZooKeeperServer NIOServerCnxnFactory]
   [java.util Properties])
  (:require [clojure.java.io :refer [file]]))

(set! *warn-on-reflection* true)

(defn safe-delete [file-path]
  (if (.exists (clojure.java.io/file file-path))
    (try
      (clojure.java.io/delete-file file-path)
      (catch Exception e (str "exception: " (.getMessage e))))
    false))

(defn delete-directory [directory-path]
  (let [directory-contents (file-seq (clojure.java.io/file directory-path))
        files-to-delete (filter #(.isFile ^java.io.File %) directory-contents)]
    (doseq [^java.io.File file files-to-delete]
      (safe-delete (.getPath file)))
    (safe-delete directory-path)))

(defn tmp-dir [& parts]
  (.getPath ^java.io.File (apply file (System/getProperty "java.io.tmpdir") "embedded-kafka" parts)))

(def ^:dynamic kafka-config
  {"broker.id"                        "0"
   "listeners"                        "PLAINTEXT://localhost:9999"
   "bootstrap.servers"                "localhost:9999"
   "zookeeper.connect"                "127.0.0.1:2182"
   "zookeeper-port"                   "2182"
   "log.flush.interval.messages"      "1"
   "auto.create.topics.enable"        "true"
   "group.id"                         "consumer"
   "auto.offset.reset"                "earliest"
   "retry.backoff.ms"                 "500"
   "message.send.max.retries"         "5"
   "auto.commit.enable"               "false"
   "offsets.topic.replication.factor" "1"
   "max.poll.records"                 "1"
   "log.dir"                          (.getAbsolutePath (file (tmp-dir "kafka-log")))
   "acks"                             "all"
   "retries"                          "0"
   "key.serializer"                   "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer"                 "org.apache.kafka.common.serialization.StringSerializer"
   "key.deserializer"                 "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer"               "org.apache.kafka.common.serialization.StringDeserializer"})

(defn as-properties [m]
  (let [ps (Properties.)]
    (doseq [[k v] m] (.setProperty ps k v))
    ps))

(defn ^KafkaServerStartable create-broker []
  (KafkaServerStartable. (KafkaConfig. (as-properties kafka-config))))

(defn ^NIOServerCnxnFactory create-zookeeper []
  (let [tick-time 500
        zk (ZooKeeperServer. (file (tmp-dir "zookeeper-snapshot")) (file (tmp-dir "zookeeper-log")) tick-time)]
    (doto (NIOServerCnxnFactory.)
      (.configure (InetSocketAddress. (read-string (kafka-config "zookeeper-port"))) 60)
      (.startup zk))))

(defmacro with-test-broker
  "Creates an in-process broker that can be used to test against"
  [producer-name consumer-name & body]
  `(do (delete-directory (file (tmp-dir)))
       (let [zk# (create-zookeeper)
             kafka# (create-broker)]
         (try
           (.startup kafka#)
           (let [^Producer ~producer-name (KafkaProducer. ^java.util.Map kafka-config)
                 ^Consumer ~consumer-name (KafkaConsumer. ^java.util.Map kafka-config)]
             (try
               ~@body
               (finally
                 (do (.close ~consumer-name)
                     (.close ~producer-name)))))
           (finally (do (.shutdown kafka#)
                        (.awaitShutdown kafka#)
                        (.shutdown zk#)
                        (delete-directory (file (tmp-dir)))))))))
