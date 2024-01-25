(ns kafka-event-processor.test-support.kafka.producer
  (:require
   [jason.convenience :as json]

   [kafka-event-processor.utils.properties :as properties])
  (:import
   [java.util Properties]
   [org.apache.kafka.common.serialization StringSerializer]
   [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
   [com.google.common.util.concurrent Futures]))

(defn producer-config [overrides]
  (properties/map->properties
    (merge
      {:bootstrap.servers  "localhost:9092"
       :acks               "1"
       :batch.size         "10"
       :client.id          "my-client"
       :request.timeout.ms "500"}
      overrides)))

(defn create-message [payload]
  (json/->wire-json payload))

(defn publish-messages
  [{:keys [broker-host broker-port]} topic messages]
  (let [key-serializer (StringSerializer.)
        value-serializer (StringSerializer.)
        ^Properties config (producer-config
                             {:bootstrap.servers
                              (str broker-host ":" broker-port)})]
    (with-open [producer (KafkaProducer.
                           config key-serializer value-serializer)]
      (do
        (.configure key-serializer config true)
        (.configure value-serializer config false)
        (mapv
          #(Futures/getUnchecked
             (.send producer (ProducerRecord. (name topic) %)))
          messages)))))
