(ns kafka-event-processor.test-support.postgres.database
  (:require [freeport.core :refer [get-free-port!]])
  (:import [com.opentable.db.postgres.embedded EmbeddedPostgres$Builder EmbeddedPostgres]))

(defn new-database
  ([] (new-database
        (get-free-port!)))
  ([port]
   (let [pgb (atom (EmbeddedPostgres/builder))
         ^EmbeddedPostgres pg (.start ^EmbeddedPostgres$Builder @pgb)]
     {:embedded-postgres   pg})))

(defn with-database [{:keys [embedded-postgres]}]
  (fn [run-tests]
    (try
      (run-tests)
      (finally
        (.close embedded-postgres)))))
