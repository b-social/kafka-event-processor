(ns kafka-event-processor.test-support.kafka.broker
  (:require
   [kafka-event-processor.utils.properties :as properties])
  (:import
   [java.nio.file Files]
   [java.nio.file.attribute FileAttribute]
   [kafka.server KafkaConfig KafkaServerStartable]))

(defn kafka-config [host port zookeeper]
  (let [port (str port)
        log-directory
        (-> (Files/createTempDirectory "kafka" (into-array FileAttribute []))
          (.toAbsolutePath)
          (.toString))]
    (KafkaConfig.
      (properties/map->properties
        {:advertised.listeners
         (str "PLAINTEXT://" host ":" port)

         :listeners
         (str "PLAINTEXT://0.0.0.0:" port)

         :port
         port

         :broker.id
         "1"

         :zookeeper.connect
         zookeeper

         :log.dirs
         log-directory

         :offsets.topic.num.partitions
         "1"

         :leader.imbalance.check.interval.seconds
         "1"

         :offsets.topic.replication.factor
         "1"

         :default.replication.factor
         "1"

         :num.partitions
         "1"

         :group.min.session.timeout.ms
         "100"}))))

(defn new-kafka-broker [config]
  (KafkaServerStartable. config))

(defn start [broker]
  (.startup broker))

(defn stop [broker]
  (.shutdown broker))
