(ns ^:no-doc kafka-event-processor.kafka.component
  (:require
    [clojure.string :as str]

    [com.stuartsierra.component :as component]

    [configurati.core
     :refer [define-configuration
             define-configuration-specification
             with-parameter
             with-source
             with-specification
             with-key-fn
             env-source]]

    [configurati.key-fns :refer [remove-prefix]]
    [configurati.conversions :refer [convert-to]])
  (:import
    [org.apache.kafka.clients.consumer ConsumerConfig]))

(defmethod convert-to :comma-separated-list [_ value]
  (cond
    (vector? value) value
    (some? value) (mapv str/trim (str/split value #","))
    :else nil))

(def kafka-configuration-specification
  (define-configuration-specification
    (with-key-fn (remove-prefix :kafka))
    (with-parameter :kafka-bootstrap-servers)
    (with-parameter :kafka-key-deserializer-class-config
      :default "org.apache.kafka.common.serialization.StringDeserializer")
    (with-parameter :kafka-value-deserializer-class-config
      :default "org.apache.kafka.common.serialization.StringDeserializer")
    (with-parameter :kafka-auto-offset-reset-config
      :default "earliest")
    (with-parameter :kafka-enable-auto-commit-config
      :default "false")
    (with-parameter :kafka-security-protocol
      :default "SSL")
    (with-parameter :kafka-ssl-truststore-location
      :default "")
    (with-parameter :kafka-ssl-truststore-password
      :default "")
    (with-parameter :kafka-ssl-keystore-location
      :default "")
    (with-parameter :kafka-ssl-keystore-password
      :default "")
    (with-parameter :kafka-ssl-key-password
      :default "")))

(defn kafka-configuration
  [prefix]
  (define-configuration
    (with-source (env-source :prefix prefix))
    (with-specification kafka-configuration-specification)))

(defrecord Kafka
  [configuration]
  component/Lifecycle

  (start [component]
    (let [{:keys [bootstrap-servers
                  key-deserializer-class-config
                  value-deserializer-class-config
                  auto-offset-reset-config
                  enable-auto-commit-config
                  security-protocol
                  ssl-truststore-location
                  ssl-truststore-password
                  ssl-keystore-location
                  ssl-keystore-password
                  ssl-key-password]} configuration

          consumer-config
          {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG
                                    bootstrap-servers
           ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG
                                    key-deserializer-class-config
           ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG
                                    value-deserializer-class-config
           ConsumerConfig/AUTO_OFFSET_RESET_CONFIG
                                    auto-offset-reset-config
           ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG
                                    enable-auto-commit-config
           :security.protocol       security-protocol
           :ssl.truststore.location ssl-truststore-location
           :ssl.truststore.password ssl-truststore-password
           :ssl.keystore.location   ssl-keystore-location
           :ssl.keystore.password   ssl-keystore-password
           :ssl.key.password        ssl-key-password}]

      (assoc component
        :consumer-config consumer-config)))

  (stop [component]
    (dissoc component :consumer-config)))

(defn new-kafka []
  (map->Kafka {}))
