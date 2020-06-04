(ns kafka-event-processor.kafka.system
  (:require
    [com.stuartsierra.component :as component]

    [configurati.core :as conf]

    [kafka-event-processor.kafka.component
     :as kafka]))

(defn new-system
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
