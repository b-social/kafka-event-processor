(ns kafka-event-processor.test-support.kafka.producer
  (:require

   [kafka-event-processor.utils.properties :as properties]
   [jason.convenience :as json])
  (:import
   [java.util Properties]
   [org.apache.kafka.common.header.internals RecordHeaders]
   [org.apache.kafka.common.serialization StringSerializer]
   [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]))

(defn producer-config [overrides]
  (properties/map->properties
    (merge
      {:bootstrap.servers  "localhost:9092"
       :acks               "1"
       :batch.size         "10"
       :client.id          "my-client"
       :request.timeout.ms "500"}
      overrides)))

(defn publish-messages
  [{:keys [broker-host broker-port]} ^String topic messages]
  (let [key-serializer (StringSerializer.)
        value-serializer (StringSerializer.)
        ^Properties config (producer-config
                             {:bootstrap.servers
                              (str broker-host ":" broker-port)})]
    (with-open [producer (KafkaProducer.
                           config key-serializer value-serializer)]
      (.configure key-serializer config true)
      (.configure value-serializer config false)
      (doseq [{:keys [headers
                      key
                      payload]} messages]
        (let [payload (json/->wire-json payload)
              ^RecordHeaders headers (reduce
                                       (fn [acc [k v]]
                                         (.add acc (name k) (.getBytes v)))
                                       (RecordHeaders.)
                                       headers)
              record (ProducerRecord. topic nil key payload headers)]
          (.get (.send producer record)))))))
