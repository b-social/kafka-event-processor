(ns kafka-event-processor.test-support.system
  (:require
    [com.stuartsierra.component :as component]
    [kafka-event-processor.test-support.database :as db]
    [kafka-event-processor.kafka.system :as kafka-system]
    [kafka-event-processor.processor.system :as processors]
    [vent.core :as vent]
    [kafka-event-processor.test-support.kafka.combined :as kafka]
    [freeport.core :refer [get-free-port!]]
    [vent.hal :as vent-hal]
    [halboy.resource :as hal]
    [halboy.json :as hal-json]
    [kafka-event-processor.processor.protocols :refer [EventHandler]]
    [clojure.string :as string]))

(defn with-system-lifecycle [system-atom]
  (fn [f]
    (try
      (do
        (reset! system-atom (component/start-system @system-atom))
        (f))
      (finally
        (reset! system-atom (component/stop-system @system-atom))))))

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


(defn- event-resource->id-from-href
  [event-resource href]
  (let [href (hal/get-href event-resource href)]
    (when-not (nil? href) (last (string/split href #"/")))))

(defn- event-resource->id
  "Get the id from the event resource"
  [event-resource]
  (event-resource->id-from-href event-resource :self))

(deftype AtomEventHandler
  [atom]
  EventHandler
  (extract-payload
    [this event]
    (-> event
      (hal-json/json->resource)
      (hal/get-property :payload)
      (hal-json/json->resource)))
  (processable?
    [this processor event event-context]
    true)
  (on-event
    [this processor {:keys [topic payload]} _]
    (vent/react-to all {:channel topic :payload payload} processor))
  (on-complete
    [this processor {:keys [topic partition payload]} {:keys [event-processor]}]
    (swap! atom conj {:processor event-processor
                      :topic     topic
                      :partition partition
                      :event-id  (event-resource->id payload)})))

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
       (AtomEventHandler. (atom []))
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
