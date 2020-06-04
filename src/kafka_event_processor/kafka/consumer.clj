(ns kafka-event-processor.kafka.consumer
  (:require
    [halboy.resource :as hal]
    [halboy.json :as hal-json]

    [kafka-event-processor.utils.logging :as log]
    [kafka-event-processor.utils.properties :refer [map->properties]]
    [kafka-event-processor.kafka.consumer-group :as kafka-consumer-group])
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

(defn new-consumer
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

(defn stop-consumer [consumer]
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

(defn extract-event-resource
  [^ConsumerRecord record]
  (-> record
    (.value)
    (hal-json/json->resource)
    (hal/get-property :payload)
    (hal-json/json->resource)))

(defn extract-events-for-topic
  [^ConsumerRecords consumer-records ^String topic]
  (let [records (->
                  (.records consumer-records topic)
                  (.iterator)
                  (iterator-seq))]
    (map
      (fn [^ConsumerRecord record]
        {:topic     topic
         :offset    (.offset record)
         :partition (.partition record)
         :resource  (extract-event-resource record)})
      records)))

(defn get-new-events
  [{:keys [^KafkaConsumer handle topics]} timeout]
  (let [records (.poll handle (Duration/ofMillis timeout))
        events
        (mapcat
          (partial extract-events-for-topic records)
          topics)]
    events))

(defn assignments [kafka-consumer]
  (let [^KafkaConsumer handle (:handle kafka-consumer)]
    (.assignment handle)))

(defn seek-to-offset [kafka-consumer event]
  (let [^KafkaConsumer handle (:handle kafka-consumer)
        partition (TopicPartition. (:topic event) (:partition event))
        ^long offset (:offset event)]
    (.seek handle partition offset)))

(defn seek-to-beginning [kafka-consumer topic-partitions]
  (let [^KafkaConsumer handle (:handle kafka-consumer)]
    (.seekToBeginning handle topic-partitions)
    (run! #(.position handle %) topic-partitions)))

(defn commit-offset
  [{:keys [^KafkaConsumer handle]}]
  (.commitSync handle))
