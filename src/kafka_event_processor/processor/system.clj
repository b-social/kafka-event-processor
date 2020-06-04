(ns kafka-event-processor.processor.system
  (:require
    [configurati.core :as conf]
    [com.stuartsierra.component :as component]
    [kafka-event-processor.utils.logging :as log]
    [kafka-event-processor.kafka.consumer-group :as kafka-consumer-group]
    [kafka-event-processor.processor.configuration :as processor-configuration]
    [kafka-event-processor.processor.component :refer [new-processor]]))

(defn- ->keyword
  [parts]
  (keyword (apply str parts)))

(defn new-system
  "Creates a new kafka event processor.

   Does nothing if processing is not enabled.

   * Processor identifier can be specified (defaults to :main).
   * Configuration prefix can be specified (defaults to :service).

   All system map keys can be overridden or they default where applicable:

   * kafka: kafka
   * database: database
   * processing-enabled: {processor-identifier}-processing-enabled?
   * kafka-consumer-group-configuration: kafka-{processor-identifier}-consumer-group-configuration
   * kafka-consumer-group: kafka-{processor-identifier}-consumer-group
   * processor-configuration: {processor-identifier}-processor-configuration
   * processor: {processor-identifier}-processor

   Optionally provide a system map key for rewind-check idempotent-check and event-handler
   
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
    (when processing-enabled?
      (component/system-map
        kafka-consumer-group-configuration
        (conf/resolve
          (kafka-consumer-group configuration-overrides
            (kafka-consumer-group/kafka-consumer-group-configuration
              configuration-prefix
              processor-identifier)))

        kafka-consumer-group
        (component/using
          (kafka-consumer-group/new-kafka-consumer-group)
          {:kafka         kafka
           :configuration kafka-consumer-group-configuration})

        processor-configuration
        (conf/resolve
          (processor configuration-overrides
            (processor-configuration/processor-configuration
              configuration-prefix
              processor-identifier)))

        processor
        (component/using
          (new-processor processor-identifier)
          (merge
            {:kafka                kafka
             :configuration        processor-configuration
             :kafka-consumer-group kafka-consumer-group
             :database             database
             :event-handler        event-handler}
            (when (some? rewind-check)
              {:rewind-check rewind-check})
            additional-dependencies))))))
