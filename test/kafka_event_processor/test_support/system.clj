(ns kafka-event-processor.test-support.system
  (:require
    [com.stuartsierra.component :as component]
    [kafka-event-processor.test-support.database :as db]
    [configurati.core :as conf]
    [kafka-event-processor.kafka.system :as kafka-system]
    [kafka-event-processor.processor.system :as processors]
    [vent.core :as vent]
    [kafka-event-processor.kafka.consumer-group :as kafka-consumer-group]
    [kafka-event-processor.utils.generators :as generators]
    [kafka-event-processor.processor.configuration :as config]
    [configurati.core
     :refer [define-configuration
             with-source
             with-specification
             map-source]]
    [configurati.key-fns :refer [remove-prefix]]
    [kafka-event-processor.test-support.kafka.combined :as kafka]
    [freeport.core :refer [get-free-port!]]
    [vent.hal :as vent-hal]
    [halboy.resource :as hal])
  (:import [kafka_event_processor.processor.component EventHandler]))

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
  (on-complete [this database cursor]
    (swap! atom conj cursor)))

(defn test-act []
  (vent/action [{:keys [id message atom]}]
    (swap! atom conj {:id id :message message})))

(defn test-gather [channel-and-payload]
  (vent/gatherer []
    (let [event (:payload channel-and-payload)]
      {:id      (hal/get-property event :id)
       :message (hal/get-property event :message)})))

(vent/defruleset all
  (vent/options
    :event-type-fn (vent-hal/event-type-property :type))
  (vent/from-channel :test
    (vent/on-type :test
      (vent/gather test-gather)
      (vent/act test-act))))

(defn new-system
  ([] (new-system {}))
  ([configuration-overrides]
   (merge
     (component/system-map
       :database-configuration
       (conf/resolve
         (:database
           configuration-overrides
           (db/database-configuration
             (select-keys configuration-overrides [:database-port]))))
       :database
       (component/using
         (db/new-database)
         {:configuration
          :database-configuration})
       :event-handler
       (AtomEventHandler. (atom []))
       :ruleset
       all
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
        :ruleset                 :ruleset
        :additional-dependencies {:atom :atom}}))))


(defn new-test-system [configuration]
  (new-system
    (merge
      {:kafka                     (kafka/kafka-configuration configuration)
       :kafka-main-consumer-group kafka/kafka-main-consumer-group-configuration
       :main-processor            kafka/main-processor-configuration
       :main-processing-enabled?  true}
      configuration)))
