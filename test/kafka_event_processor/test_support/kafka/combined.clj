(ns kafka-event-processor.test-support.kafka.combined
  (:require
    [configurati.core
     :refer [define-configuration
             with-specification
             with-source
             map-source]]

    [freeport.core :refer [get-free-port!]]

    [kafka-event-processor.test-support.kafka.zookeeper
     :as zk]
    [kafka-event-processor.test-support.kafka.broker
     :as broker]
    [kafka-event-processor.kafka.consumer-group :as kafka-consumer-group]
    [kafka-event-processor.utils.generators :as generators]
    [kafka-event-processor.processor.configuration :as config]))

(defn new-kafka
  ([] (new-kafka
        "localhost"
        (get-free-port!)))
  ([host port]
    (let [zookeeper (atom (zk/new-zookeeper))
          broker-config (broker/kafka-config host port
                          (zk/connect-string @zookeeper))
          broker (atom (broker/new-kafka-broker broker-config))]
      {:broker      broker
       :broker-host host
       :broker-port port
       :zookeeper   zookeeper})))

(defn start [{:keys [zookeeper broker]}]
  (do
    (zk/start @zookeeper)
    (broker/start @broker)))

(defn stop [{:keys [zookeeper broker]}]
  (do
    (broker/stop @broker)
    (zk/stop @zookeeper)))

(defn with-kafka [kafka]
  (fn [run-tests]
    (try
      (do
        (start kafka)
        (run-tests))
      (finally
        (stop kafka)))))

(defn kafka-configuration
  [{:keys [broker-host broker-port]}]
  {:bootstrap.servers (str broker-host ":" broker-port)
   :auto.offset.reset "earliest"
   :security.protocol "PLAINTEXT"})

(def kafka-main-consumer-group-configuration
  (define-configuration
    (with-specification
      (kafka-consumer-group/kafka-consumer-group-configuration-specification :main))
    (with-source
      (map-source
        {:kafka-main-consumer-group-id     (str (generators/uuid))
         :kafka-main-consumer-group-topics ["test"]}))))

(def main-processor-configuration
  (define-configuration
    (with-source
      (map-source {}))
    (with-specification
      (config/processor-configuration-specification :main))))
