(ns kafka-event-processor.kafka.system
  (:require
    [com.stuartsierra.component :as component]

    [configurati.core :as conf]

    [kafka-event-processor.kafka.component
     :as kafka]
    [kafka-event-processor.utils.logging :as log]
    [malli.core :as m]
    [malli.error :as me]))


(def kafka-configuration-schema
  (m/schema
    [:map
     [:security-protocol :string]
     [:ssl-keystore-password :string]
     [:ssl-keystore-location :string]
     [:ssl-truststore-password :string]
     [:value-deserializer-class-config [:enum "org.apache.kafka.common.serialization.StringDeserializer"]]
     [:key-deserializer-class-config [:enum "org.apache.kafka.common.serialization.StringDeserializer"],]
     [:ssl-key-password :string]
     [:enable-auto-commit-config [:enum "false" "true"]]
     [:bootstrap-servers :string]
     [:ssl-truststore-location :string]
     [:auto-offset-reset-config [:enum "earliest" "latest" "none"]]]))

(def valid-kafka-configuration?
  (m/validator kafka-configuration-schema))

(defn- config-errors
  [config]
  (me/humanize (m/explain kafka-configuration-schema config)))

#_(comment
  ;; this map is what the result of calling:
  (conf/resolve
    (:kafka configuration-overrides
      (kafka/kafka-configuration configuration-prefix)))
  ;; result
  {:ssl-keystore-password "",
   :security-protocol "SSL",
   :ssl-keystore-location "",
   :ssl-truststore-password "",
   :value-deserializer-class-config "org.apache.kafka.common.serialization.StringDeserializer",
   :ssl-key-password "",
   :enable-auto-commit-config "false",
   :bootstrap-servers "\"SSL://event-bus-kafka-1-thorium.development-cobalt.b-anti-social.io:9092,SSL://event-bus-kafka-2-thorium.development-cobalt.b-anti-social.io:9092,SSL://event-bus-kafka-3-thorium.development-cobalt.b-anti-social.io:9092\"",
   :ssl-truststore-location "",
   :key-deserializer-class-config "org.apache.kafka.common.serialization.StringDeserializer",
   :auto-offset-reset-config "earliest"}

  ;; the idea is to add an override that you can pass from the service, so we skip the configurati resolve step here
  ;; in addition to that there is Malli schema to validate `(:kafka-configuration configuration-overrides)`
  ;; so it's in correct format

  ;; building this map will be done in the service itself, where we can use new library to fetch certs
  ;; and we can use aero to have an explicit definition how it is mapped to env variables
  )

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
   (let [kafka-enabled? (get configuration-overrides kafka-enabled true)
         kafka-configuration-value (or (:kafka-configuration configuration-overrides)
                                 (conf/resolve
                                   (:kafka configuration-overrides
                                     (kafka/kafka-configuration configuration-prefix))))]
     (println "HERE!" kafka-configuration-value)
     (when-not (valid-kafka-configuration? kafka-configuration-value)
       (throw (ex-info "Input config is not valid against the schema"
                       {:errors (config-errors kafka-configuration-value)})))
     (log/log-info {kafka-enabled kafka-enabled?} "Kafka enabled?")
     (when kafka-enabled?
       (component/system-map
         kafka-configuration kafka-configuration-value

         kafka
         (component/using
           (kafka/new-kafka)
           {:configuration kafka-configuration}))))))