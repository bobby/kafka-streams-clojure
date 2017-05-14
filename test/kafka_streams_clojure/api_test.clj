(ns kafka-streams-clojure.api-test
  (:require [clojure.test :refer :all]
            [kafka-streams-clojure.embedded-kafka :refer [with-test-broker kafka-config]]
            [kafka-streams-clojure.api :as api])
  (:import [kafka-streams-clojure.api ]
           [org.apache.kafka.clients.producer Producer ProducerRecord]
           [org.apache.kafka.clients.consumer Consumer ConsumerRecords ConsumerRecord]
           [org.apache.kafka.streams KafkaStreams StreamsConfig]
           [org.apache.kafka.streams.kstream KStreamBuilder Transformer TransformerSupplier]
           [org.apache.kafka.streams.processor Processor ProcessorSupplier]))

(set! *warn-on-reflection* true)

(deftest test-kafka-streams-api-types
  (testing "Integration with Kafka Streams API types"
    (let [xform     (comp (filter (fn [[k v]] (string? v)))
                          (map (fn [[k v]] [v k]))
                          (filter (fn [[k v]] (= "foo" v))))
          supplier  (api/processor-supplier xform)
          processor (api/processor xform)]
      (is (instance? TransformerSupplier supplier))
      #_(is (instance? ProcessorSupplier   supplier))

      (is (instance? Transformer processor))
      #_(is (instance? Processor   processor))

      (is (instance? Transformer (.get supplier)))
      #_(is (instance? Processor   (.get supplier))))))

(defn produce-records-synchronously
  [^Producer producer topic records]
  (doseq [[k v] records]
    @(.send producer (ProducerRecord. topic k v))))

(defn consume-n-records
  [^Consumer consumer topic n]
  (.subscribe consumer [topic])
  (loop [records []]
    (if (< (count records) n)
      (recur (reduce (fn [agg ^ConsumerRecord record] (conj agg [(.key record) (.value record)]))
                     records
                     (.poll consumer 1000)))
      (take n records))))

(deftest test-integration
  (testing "Basic integration test"
    (with-test-broker producer consumer
      (let [input-topic     "tset"
            output-topic    "test"
            xform           (comp (filter (fn [[k v]] (string? v)))
                                  (map (fn [[k v]] [v k]))
                                  (filter (fn [[k v]] (= "foo" v))))
            builder         (KStreamBuilder.)
            kstream         (-> builder
                                (.stream (into-array String ["tset"]))
                                (api/transduce-kstream xform)
                                (.to "test"))
            kafka-streams   (KafkaStreams. builder (StreamsConfig. {StreamsConfig/APPLICATION_ID_CONFIG    "test-app-id"
                                                                    StreamsConfig/BOOTSTRAP_SERVERS_CONFIG (get kafka-config "bootstrap.servers")
                                                                    StreamsConfig/KEY_SERDE_CLASS_CONFIG   org.apache.kafka.common.serialization.Serdes$StringSerde
                                                                    StreamsConfig/VALUE_SERDE_CLASS_CONFIG org.apache.kafka.common.serialization.Serdes$StringSerde}))
            input-values    {"foo" "bar"
                             "baz" "quux"}]
        (.start kafka-streams)

        (produce-records-synchronously producer input-topic input-values)
        (is (= [["bar" "foo"]] (consume-n-records consumer output-topic 1)))

        (.close kafka-streams)))))
