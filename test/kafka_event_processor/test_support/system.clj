(ns kafka-event-processor.test-support.system
  (:require
   [com.stuartsierra.component :as component]
   [kafka-event-processor.test-support.database :as db]
   [kafka-event-processor.kafka.system :as kafka-system]
   [kafka-event-processor.processor.system :as processors]
   [kafka-event-processor.test-support.kafka.combined :as kafka]
   [jason.convenience :as json]
   [kafka-event-processor.processor.protocols :refer [EventHandler]]))

(defn with-system-lifecycle [system-atom]
  (fn [f]
    (try
      (do
        (reset! system-atom (component/start-system @system-atom))
        (f))
      (finally
        (reset! system-atom (component/stop-system @system-atom))))))

(deftype AtomEventHandler
         [atom]
  EventHandler
  (extract-payload
    [this event]
    (json/<-wire-json event))
  (processable?
    [this processor event event-context]
    true)
  (on-event
    [this processor event _]
    (swap! (:atom processor) conj event))
  (on-complete
    [this processor {:keys [topic partition payload]} {:keys [event-processor]}]
    (swap! atom conj {:processor event-processor
                      :topic     topic
                      :partition partition
                      :payload payload})))

(defn new-system
  ([] (new-system {}))
  ([configuration-overrides]
   (merge
     (component/system-map
       :embedded-postgres (:embedded-postgres configuration-overrides)

       :database
       (component/using
         (db/new-database)
         {:db
          :embedded-postgres})

       :event-handler
       (or (:event-handler configuration-overrides)
         (AtomEventHandler. (atom [])))

       :atom
       (atom []))
     (kafka-system/new-system
       configuration-overrides
       {:kafka :kafka})
     (processors/new-system
       configuration-overrides
       {:processor-identifier    :main
        :kafka                   :kafka
        :database                :database
        :event-handler           :event-handler
        :additional-dependencies {:atom :atom}}))))

(defn new-test-system [configuration]
  (new-system
    (merge
      {:kafka                     (kafka/kafka-configuration configuration)
       :kafka-main-consumer-group kafka/kafka-main-consumer-group-configuration
       :main-processor            kafka/main-processor-configuration
       :main-processing-enabled?  true}
      configuration)))
