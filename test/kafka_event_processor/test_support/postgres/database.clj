(ns kafka-event-processor.test-support.postgres.database
  (:require [freeport.core :refer [get-free-port!]])
  (:import [com.opentable.db.postgres.embedded EmbeddedPostgres$Builder EmbeddedPostgres]))

(defn new-database
  ([] (new-database
        (get-free-port!)))
  ([port]
   (let [pgb (atom (-> (EmbeddedPostgres/builder)
                     (.setPort port)))]
     {:database-port port
      :db            pgb})))

(defn with-database [{:keys [db]}]
  (fn [run-tests]
    (let [^EmbeddedPostgres pg (.start ^EmbeddedPostgres$Builder @db)]
      (try
        (run-tests)
        (finally
          (.close pg))))))
