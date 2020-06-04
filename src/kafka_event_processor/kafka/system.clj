(ns kafka-event-processor.kafka.system
  (:require
    [com.stuartsierra.component :as component]

    [configurati.core :as conf]

    [kafka-event-processor.kafka.component
     :as kafka]
    [kafka-event-processor.utils.logging :as log]))

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
    {:keys [kafka kafka-configuration configuration-prefix kafka-enabled]
     :or   {kafka                :kafka
            kafka-configuration  :kafka-configuration
            kafka-enabled        :kafka-enabled?
            configuration-prefix :service}}]
   (let [kafka-enabled?
         (get configuration-overrides kafka-enabled true)]
     (log/log-info
       {kafka-enabled kafka-enabled?}
       "Kafka enabled?")
     (when kafka-enabled?
       (component/system-map
         kafka-configuration
         (conf/resolve
           (:kafka configuration-overrides
             (kafka/kafka-configuration configuration-prefix)))

         kafka
         (component/using
           (kafka/new-kafka)
           {:configuration kafka-configuration}))))))
