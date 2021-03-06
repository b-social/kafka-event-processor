(ns kafka-event-processor.processor.consumer
  (:require
    [kafka-event-processor.utils.logging :as log]
    [kafka-event-processor.utils.properties :refer [map->properties]]
    [kafka-event-processor.kafka.consumer-group :as kafka-consumer-group]
    [kafka-event-processor.processor.protocols :refer [extract-payload]])
  (:import
    [org.apache.kafka.clients.consumer KafkaConsumer
                                       ConsumerRebalanceListener
                                       ConsumerRecord
                                       ConsumerRecords]
    [org.apache.kafka.common TopicPartition]
    [java.util Collection]
    [java.time Duration]))

(defrecord FnBackedConsumerRebalanceListener [kafka-consumer callbacks]
  ConsumerRebalanceListener
  (onPartitionsRevoked [_ topic-partitions]
    ((or (:on-partitions-revoked callbacks) (fn [_ _]))
     kafka-consumer topic-partitions))
  (onPartitionsAssigned [_ topic-partitions]
    ((or (:on-partitions-assigned callbacks) (fn [_ _]))
     kafka-consumer topic-partitions)))

(defn ^:no-doc new-consumer
  ([config topics]
   (new-consumer config topics {}))
  ([config ^Collection topics callbacks]
   (let [props (map->properties config)
         consumer (KafkaConsumer. props)
         kafka-consumer {:handle consumer :topics topics}]
     (.subscribe consumer topics
       ^ConsumerRebalanceListener
       (->FnBackedConsumerRebalanceListener kafka-consumer callbacks))
     kafka-consumer)))

(defn ^:no-doc stop-consumer [consumer]
  (try
    (log/log-info {:topics (:topics consumer)}
      "Stopping kafka consumer.")
    (.close ^KafkaConsumer (:handle consumer))
    (catch Exception e
      (log/log-error
        {:exception e}
        "Error while stopping kafka consumer."))))

(defmacro with-consumer [binding-details & body]
  `(let [consumer-group# ~(second binding-details)
         consumer-config# (kafka-consumer-group/consumer-config-for consumer-group#)
         topics# (get-in consumer-group# [:configuration :topics])
         callbacks# (or (:callbacks consumer-group#) {})
         consumer# (new-consumer consumer-config# topics# callbacks#)

         ~(first binding-details) consumer#]
     (try
       ~@body
       (finally
         (stop-consumer consumer#)))))

(defn- extract-event-resource
  [^ConsumerRecord record event-handler]
  (extract-payload event-handler (.value record)))

(defn- extract-events-for-topic
  [^ConsumerRecords consumer-records event-handler ^String topic]
  (let [records (->
                  (.records consumer-records topic)
                  (.iterator)
                  (iterator-seq))]
    (map
      (fn [^ConsumerRecord record]
        {:topic     topic
         :offset    (.offset record)
         :partition (.partition record)
         :payload   (extract-event-resource record event-handler)})
      records)))

(defn get-new-events
  "Reads events from kafka."
  [{:keys [^KafkaConsumer handle topics]} timeout event-handler]
  (let [records (.poll handle (Duration/ofMillis timeout))
        events
        (mapcat
          (partial extract-events-for-topic records event-handler)
          topics)]
    events))

(defn assignments
  "Gets assignment from a KafkaConsumer"
  [kafka-consumer]
  (let [^KafkaConsumer handle (:handle kafka-consumer)]
    (.assignment handle)))

(defn seek-to-offset
  "Seek to the offset of a specific event in a Kafka topic"
  [kafka-consumer event]
  (let [^KafkaConsumer handle (:handle kafka-consumer)
        partition (TopicPartition. (:topic event) (:partition event))
        ^long offset (:offset event)]
    (.seek handle partition offset)))

(defn seek-to-beginning
  "Seek to the beginning of a Kafka topic"
  [kafka-consumer topic-partitions]
  (let [^KafkaConsumer handle (:handle kafka-consumer)]
    (.seekToBeginning handle topic-partitions)
    (run! #(.position handle %) topic-partitions)))

(defn commit-offset
  "Commit the offset to a KafkaConsumer"
  [{:keys [^KafkaConsumer handle]}]
  (.commitSync handle))
