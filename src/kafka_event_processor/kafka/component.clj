(ns kafka-event-processor.kafka.component
  (:require [com.stuartsierra.component :as component]
            [malli.core :as m]
            [malli.error :as me]
            [malli.util :as mu]))

(def kafka-base-configuration-schema
  (m/schema
    [:map
     [:bootstrap.servers :string]
     [:auto.offset.reset [:enum "earliest" "latest" "none"]]
     [:key.deserializer {:optional true} :string]
     [:value.deserializer {:optional true} :string]
     [:enable.auto.commit {:optional true} [:enum "false" "true"]]]))

(def kafka-configuration-schema
  (m/schema
    [:multi {:dispatch :security.protocol}
     ["SSL"
      (mu/merge
        kafka-base-configuration-schema
        [:map
         [:security.protocol [:enum "SSL"]]
         [:ssl.keystore.location :string]
         [:ssl.keystore.password :string]
         [:ssl.key.password :string]
         [:ssl.truststore.location :string]
         [:ssl.truststore.password :string]])]
     ["PLAINTEXT"
      (mu/merge
        kafka-base-configuration-schema
        [:map [:security.protocol [:enum "PLAINTEXT"]]])]]))

(def valid-configuration?
  (m/validator kafka-configuration-schema))

(defn- configuration-errors
  [configuration]
  (->> configuration
       (m/explain kafka-configuration-schema)
       (me/humanize)))

(defrecord Kafka
  [configuration]
  component/Lifecycle

  (start [component]
    (when-not (valid-configuration? configuration)
      (throw (ex-info "Input config is not valid against the schema"
                      {:errors (configuration-errors configuration)})))
    (assoc component
           :consumer-config
           (merge
             {:key.deserializer "org.apache.kafka.common.serialization.StringDeserializer"
              :value.deserializer "org.apache.kafka.common.serialization.StringDeserializer"
              :enable.auto.commit "false"}
             configuration)))

  (stop [component]
    (dissoc component :consumer-config)))
(defn new-kafka
  [configuration]
  (map->Kafka {:configuration configuration}))