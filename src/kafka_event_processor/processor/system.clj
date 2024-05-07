(ns kafka-event-processor.processor.system
  (:require
   [configurati.core :as conf]
   [com.stuartsierra.component :as component]
   [kafka-event-processor.utils.logging :as log]
   [kafka-event-processor.kafka.consumer-group :as kafka-consumer-group]
   [kafka-event-processor.processor.configuration :as processor-configuration]
   [com.kroo.service-base.kafka-event-processor :as kep]
   [clojure.string :as str]))

(defn- ->keyword
  [parts]
  (keyword (str/join parts)))

;; we have to make this a component
;; so we can access other parts of the system to build up the config
+(defrecord ProcessorConfiguration
    [consumer-configuration consumer-group-configuration]
  component/Lifecycle
  
  (start [component]
    (merge
     component
     {:kafka-consumer (assoc (:consumer-config consumer-configuration)
                             :group.id (:id consumer-group-configuration))
      :topics (set (:topics consumer-group-configuration))}))
  
  (stop [component]
    component))


(defn new-system
  "Creates a new kafka event processor.

   Does nothing if processing is not enabled.

   * Processor identifier can be specified (defaults to :main).
   * Configuration prefix can be specified (defaults to :service).

   All system map keys can be overridden or they default where applicable:

   * kafka: kafka (mandatory)
   * database: database (mandatory)
   * event-handler: {processor-identifier}-event-handler (mandatory)
   * processing-enabled: {processor-identifier}-processing-enabled?
   * kafka-consumer-group-configuration: kafka-{processor-identifier}-consumer-group-configuration
   * kafka-consumer-group: kafka-{processor-identifier}-consumer-group
   * processor-configuration: {processor-identifier}-processor-configuration
   * processor: {processor-identifier}-processor

   Optionally provide a system map key for rewind-check

   Optional provide a map of system keys that are used as additional dependencies to the component

   e.g.

   ````
   (processors/new-system
     configuration-overrides
     {:processor-identifier    :main
      :kafka                   :kafka
      :database                :database
      :event-handler           :event-handler
      :additional-dependencies {:atom :atom}})
   ````

   Nothing is done with the event if an event-handler is not defined.

   ````
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
  ````"
  [configuration-overrides
   {:keys [kafka database processor-identifier configuration-prefix additional-dependencies
           processing-enabled kafka-consumer-group-configuration kafka-consumer-group
           processor-configuration processor rewind-check event-handler]
    :or   {kafka                   :kafka
           database                :database
           processor-identifier    :main
           configuration-prefix    :service
           additional-dependencies {}}}]
  (let [processor-name
        (name processor-identifier)
        event-handler
        (or event-handler (->keyword [processor-name "-event-handler"]))
        processing-enabled
        (or processing-enabled (->keyword [processor-name "-processing-enabled?"]))
        kafka-consumer-group-configuration
        (or kafka-consumer-group-configuration
          (->keyword ["kafka-" processor-name "-consumer-group-configuration"]))
        kafka-consumer-group
        (or kafka-consumer-group (->keyword ["kafka-" processor-name "-consumer-group"]))
        processor-configuration
        (or processor-configuration (->keyword [processor-name "-processor-configuration"]))
        processor
        (or processor (->keyword [processor-name "-processor"]))
        processing-enabled?
        (get configuration-overrides processing-enabled true)]
    (log/log-info
      {processing-enabled processing-enabled?}
      "Processing enabled?")
    (component/system-map
     kafka-consumer-group-configuration
     (conf/resolve
      (kafka-consumer-group configuration-overrides
                            (kafka-consumer-group/kafka-consumer-group-configuration
                             configuration-prefix
                             processor-identifier)))

     ;; this component is a total no-op
     ;; (except it contains these other things we're referring to here)
     ;; it's no longer used by this library, but leaving for backwards compatibility
     kafka-consumer-group
     (component/using
      (kafka-consumer-group/new-kafka-consumer-group)
      {:kafka         kafka
       :configuration kafka-consumer-group-configuration})

     processor-configuration
     (component/using 
      (map->ProcessorConfiguration
       (merge 
        {:enabled? processing-enabled?
         :processor-id processor-identifier}
        (conf/resolve
         (processor configuration-overrides
                    (processor-configuration/processor-configuration
                     configuration-prefix
                     processor-identifier)))))
      {:consumer-configuration kafka
       :consumer-group-configuration kafka-consumer-group-configuration})

     processor
     (component/using
      (kep/map->KafkaEventProcessor {})
      (merge {:configuration        processor-configuration
              :database             database
              :event-handler        event-handler}
             (when (some? rewind-check)
               {:rewind-check rewind-check})
             additional-dependencies)))))
