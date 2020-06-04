(ns kafka-event-processor.kafka.system
  (:require
    [com.stuartsierra.component :as component]

    [configurati.core :as conf]

    [kafka-event-processor.kafka.component
     :as kafka]))

(defn new-system
  "Creates a new kafka consumer client.

   * Configuration prefix to be specified (defaults to :service).

   All system map keys can be overridden or they default where applicable:

   * kafka: :kafka
   * kafka-configuration: :kafka-configuration

   e.g.
   
   ````
   (kafka-system/new-system
          configuration-overrides
          {:kafka :kafka})
   ````
   "
  ([configuration-overrides]
   (new-system configuration-overrides {}))
  ([configuration-overrides
    {:keys [kafka kafka-configuration configuration-prefix]
     :or   {kafka                :kafka
            kafka-configuration  :kafka-configuration
            configuration-prefix :service}}]

   (component/system-map
     kafka-configuration
     (conf/resolve
       (:kafka configuration-overrides
         (kafka/kafka-configuration configuration-prefix)))

     kafka
     (component/using
       (kafka/new-kafka)
       {:configuration kafka-configuration}))))
