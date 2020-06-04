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
  [configuration-overrides
   {:keys [kafka database processor-identifier configuration-prefix additional-dependencies
           processing-enabled kafka-consumer-group-configuration kafka-consumer-group
           processor-configuration processor rewind-check idempotent-check event-handler
           ruleset]
    :or   {kafka                   :kafka
           database                :database
           processor-identifier    :main
           configuration-prefix    :service
           additional-dependencies {}}}]
  (let [processor-name
        (name processor-identifier)
        ruleset
        (or ruleset (->keyword [processor-name "-ruleset"]))
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
             :ruleset              ruleset}
            (when (some? rewind-check)
              {:rewind-check rewind-check})
            (when (some? idempotent-check)
              {:idempotent-check idempotent-check})
            (when (some? event-handler)
              {:event-handler event-handler})
            additional-dependencies))))))
