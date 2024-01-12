(ns kafka-event-processor.test-support.kafka.zookeeper
  (:require
   [freeport.core :refer [get-free-port!]])
  (:import
   [org.apache.curator.test TestingServer]))

(defn new-zookeeper
  ([] (new-zookeeper (get-free-port!)))
  ([^Integer port]
   (TestingServer. port false)))

(defn connect-string [zookeeper]
  (.getConnectString zookeeper))

(defn start [zookeeper]
  (.start zookeeper))

(defn stop [zookeeper]
  (.close zookeeper))
