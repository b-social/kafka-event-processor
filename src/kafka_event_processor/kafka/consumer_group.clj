(ns kafka-event-processor.kafka.consumer-group
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
    [configurati.conversions :refer [convert-to]]

    [kafka-event-processor.utils.properties :refer [map->properties]])
  (:import [org.apache.kafka.clients.consumer ConsumerConfig]))

(defmethod convert-to :comma-separated-list [_ value]
  (cond
    (vector? value) value
    (some? value) (mapv str/trim (str/split value #","))
    :else nil))

(defn kafka-consumer-group-configuration-specification [consumer-group-type]
  (let [prefix (str "kafka-" (name consumer-group-type) "-consumer-group")]
    (letfn [(prefixed-parameter [parameter-name]
              (keyword (str prefix "-" (name parameter-name))))]
      (define-configuration-specification
        (with-key-fn (remove-prefix (keyword prefix)))
        (with-parameter (prefixed-parameter :id))
        (with-parameter (prefixed-parameter :topics)
          :type :comma-separated-list)))))

(defn kafka-consumer-group-configuration [prefix consumer-group-type]
  (define-configuration
    (with-source (env-source :prefix prefix))
    (with-specification
      (kafka-consumer-group-configuration-specification consumer-group-type))))

(defn consumer-config-for [kafka-consumer-group]
  (merge
    {ConsumerConfig/GROUP_ID_CONFIG (get-in kafka-consumer-group [:configuration :id])}
    (get-in kafka-consumer-group [:kafka :consumer-config])))

(defrecord KafkaConsumerGroup
           []
  component/Lifecycle

  (start [component]
    component)

  (stop [component]
    component))

(defn new-kafka-consumer-group []
  (map->KafkaConsumerGroup {}))
