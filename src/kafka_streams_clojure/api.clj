(ns kafka-streams-clojure.api
  (:import [org.apache.kafka.streams.kstream
            KStreamBuilder
            Transformer
            TransformerSupplier
            KStream
            Predicate
            ValueJoiner
            KeyValueMapper]
           [org.apache.kafka.streams.processor ProcessorContext]
           [org.apache.kafka.streams KeyValue]
           [org.apache.kafka.streams.state KeyValueIterator ReadOnlyKeyValueStore]))

(set! *warn-on-reflection* true)

(deftype TransducerTransformer [step-fn ^{:volatile-mutable true} context]
  Transformer
  (init [_ c]
    (set! context c))
  (transform [_ k v]
    (try
      (step-fn context [k v])
      (catch Exception e
        (.printStackTrace e)))
    nil)
  (punctuate [^Transformer this ^long t])
  (close [_]))

(defn- kafka-streams-step
  ([context] context)
  ([^ProcessorContext context [k v]]
   (.forward context k v)
   (.commit context)
   context))

(defn transformer
  "Creates a transducing transformer for use in Kafka Streams topologies."
  [xform]
  (TransducerTransformer. (xform kafka-streams-step) nil))

(defn transformer-supplier
  [xform]
  (reify
    TransformerSupplier
    (get [_] (transformer xform))))

;;; TODO: Wrap fluent build DSL in something more Clojure-y (macro?!?!?)

(defn transduce-kstream
  ^KStream [^KStream kstream xform]
  (.transform kstream (transformer-supplier xform) (into-array String [])))

;;; TODO: Reproduce useful KStream, KTable APIs here as transducers
;;; (i.e. for those not already covered by existing map, filter, etc.)
;;; (e.g. leftJoin, through, etc.)

(defn ^KStream stream
  "Clojure wrapper around KStreamBuilder.stream(String names...)"
  [^KStreamBuilder builder & stream-names]
  (.stream builder (into-array String stream-names)))

(defn branch
  "Clojure wrapper around KStream.branch(Predicate predicates...).
  Accepts a KStream instance and a variable number of arity-1
  predicates of [k v]."
  [^KStream kstream & predicates]
  (let [preds (into-array Predicate (map #(reify Predicate (test [_ k v] (% [k v]))) predicates))]
    (into [] (.branch kstream preds))))

(defn branch-map
  "Given a KStream instance and a map of

  `keyword-branch-name -> (arity-1 predicate of [k v])`

   returns a map of

  `keyword-branch-name -> KStream`

  as per KStream.branch"
  [^KStream kstream branch-predicate-map]
  (let [[branch-names predicates] (reduce (fn [agg [k v]]
                                            (-> agg
                                                (update-in [0] conj k)
                                                (update-in [1] conj v)))
                                          [[] []]
                                          branch-predicate-map)
        kstreams (apply branch kstream predicates)]
    (zipmap branch-names kstreams)))

(defn ^ValueJoiner value-joiner
  [f]
  (reify ValueJoiner
    (apply [_ v1 v2]
      (f v1 v2))))

(defn ^KeyValueMapper key-value-mapper
  [f]
  (reify KeyValueMapper
    (apply [_ k v]
      (f [k v]))))

(defprotocol IReadOnlyKeyValueStore
  (-get [this key]
    "Gets the value at the given key.")
  (-all [this]
    "Returns a Seqable of all `[key value]` pairs in this store.  The
    Seqable is also Closeable, and must be `.close`d after use.")
  (-range [this start end]
    "Returns a Seqable of `[key value]` pairs between keys `start` and
    `end`.  The Seqable is also Closeable, and must be `.close`d after
    use."))

(defn get
  "Gets the value at the given key."
  [this db-name]
  (-get this db-name))

(defn all
  "Returns a Seqable of all `[key value]` pairs in this store.  The
  Seqable is also Closeable, and must be `.close`d after use."
  [this]
  (-all this))

(defn range
  "Returns a Seqable of `[key value]` pairs between keys `start` and
  `end`.  The Seqable is also Closeable, and must be `.close`d after
  use."
  [this start end]
  (-range this start end))

(deftype KeyValueTupleIterator [^KeyValueIterator iter]
  KeyValueIterator
  (hasNext [_] (.hasNext iter))
  (next [_]
    (let [^KeyValue kv (.next iter)]
      [(.key kv) (.value kv)]))
  (remove [_] (throw (UnsupportedOperationException. "Not supported")))
  (close [_] (.close iter))
  (peekNextKey [_] (.peekNextKey iter)))

(extend-type ReadOnlyKeyValueStore
  IReadOnlyKeyValueStore
  (-get [this _key]
    (.get this _key))
  (-all [this]
    (-> this
        .all
        ->KeyValueTupleIterator))
  (-range [this start end]
    (-> this
        (.range start end)
        ->KeyValueTupleIterator)))

(comment
  (import '[org.apache.kafka.streams StreamsConfig KafkaStreams])

  (def xform (comp (filter (fn [[k v]] (string? v)))
                   (map (fn [[k v]] [v k]))
                   (filter (fn [[k v]] (= "foo" v)))))
  (def builder (KStreamBuilder.))
  (def kstream (-> builder
                   (stream "tset")
                   (transduce-kstream xform)
                   (.to "test")))

  (def kafka-streams
    (KafkaStreams. builder (StreamsConfig. {StreamsConfig/APPLICATION_ID_CONFIG    "test-app-id"
                                            StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9092"
                                            StreamsConfig/KEY_SERDE_CLASS_CONFIG   org.apache.kafka.common.serialization.Serdes$StringSerde
                                            StreamsConfig/VALUE_SERDE_CLASS_CONFIG org.apache.kafka.common.serialization.Serdes$StringSerde})))
  (.start kafka-streams)

  (import '[org.apache.kafka.clients.producer KafkaProducer ProducerRecord])

  (def producer (KafkaProducer. {"bootstrap.servers" "localhost:9092"
                                 "acks"              "all"
                                 "retries"           "0"
                                 "key.serializer"    "org.apache.kafka.common.serialization.StringSerializer"
                                 "value.serializer"  "org.apache.kafka.common.serialization.StringSerializer"}))

  @(.send producer (ProducerRecord. "tset" "foo" "bar"))

  (.close producer)
  (.close kafka-streams)

  )
