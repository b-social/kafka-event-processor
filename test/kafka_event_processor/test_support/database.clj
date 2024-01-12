(ns kafka-event-processor.test-support.database
  (:require
   [com.stuartsierra.component :as component])
  (:import [java.io Closeable]))

(defrecord Database
           [db handle]
  component/Lifecycle

  (start [component]
    (assoc component :handle {:db         db
                              :datasource (.getPostgresDatabase db)}))

  (stop [component]
    (let [^Closeable datasource (get-in component [:handle :db])]
      (when datasource
        (.close datasource))
      (assoc component :handle nil))))

(defn new-database []
  (map->Database {}))