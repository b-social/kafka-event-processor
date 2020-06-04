(ns kafka-event-processor.processor.component
  (:require [com.stuartsierra.component :as component]
            [kafka-event-processor.utils.logging :as log]
            [kafka-event-processor.kafka.consumer :as kafka-consumer]
            [clojure.java.jdbc :as jdbc]
            [kafka-event-processor.utils.generators :as generate]
            [kafka-event-processor.processor.source
             :refer [event->topic
                     event->partition
                     event->topic-and-id
                     event-resource->id]]
            [vent.core :as vent]))

(defn- milliseconds [millis] millis)

(defmacro every [millis & body]
  `(while (not (Thread/interrupted))
     ~@body
     (Thread/sleep ~millis)))

(defprotocol RewindCheck
  :extend-via-metadata true
  (rewind-required? [this processor]))

(defprotocol IdempotentCheck
  :extend-via-metadata true
  (processable? [this database topic event-id]))

(defprotocol EventHandler
  :extend-via-metadata true
  (on-complete [this database cursor]))

(defn- process-events-once
  [{:keys [configuration kafka-consumer database ruleset event-processor
           ^IdempotentCheck idempotent-check ^EventHandler event-handler]
    :as   processor}]
  (log/log-debug
    {:event-processor event-processor
     :assignments     (kafka-consumer/assignments kafka-consumer)}
    "Checking for new event batch.")
  (let [{:keys [timeout]} configuration
        events (kafka-consumer/get-new-events kafka-consumer timeout)
        event-identifiers (map event->topic-and-id events)
        event-processing-batch-id (generate/uuid)
        event-processing-batch-context
        {:event-processor           event-processor
         :event-processing-batch-id event-processing-batch-id
         :event-identifiers         event-identifiers}]
    (when (pos? (count events))
      (log/log-info event-processing-batch-context
        "Starting processing of event batch.")
      (doseq [{:keys [topic resource partition] :as event} events
              :let [event-id (event-resource->id resource)
                    event-identifier (event->topic-and-id topic event-id)
                    event-context
                    {:event-processor           event-processor
                     :event-processing-batch-id event-processing-batch-id
                     :event-identifier          event-identifier}]]
        (try
          (log/log-info event-context "Starting processing of event.")
          (jdbc/with-db-transaction [transaction (:handle database)]
            (let [database (assoc database :handle transaction)]
              (if (or
                    (nil? idempotent-check)
                    (processable? idempotent-check database topic event-id))
                (do
                  (log/log-info event-context
                    "Continuing processing of event: not yet processed.")
                  (vent/react-to ruleset
                    {:channel topic :payload resource}
                    (into {} processor))
                  (on-complete event-handler database {:processor event-processor
                                                     :topic     topic
                                                     :partition partition
                                                     :event-id  event-id})
                  (log/log-info event-context "Completed processing of event."))
                (log/log-warn event-context
                  "Skipping processing of event: already processed."))))
          (catch Throwable exception
            (log/log-error
              event-context
              "Error processing event."
              exception)
            (kafka-consumer/seek-to-offset kafka-consumer event)
            (throw exception))))
      (kafka-consumer/commit-offset kafka-consumer)
      (log/log-info event-processing-batch-context
        "Completed processing of event batch."))))

(defn- process-events-forever
  [{:keys [configuration kafka-consumer-group ^RewindCheck rewind-check event-processor]
    :as   processor}]
  (let [{:keys [interval]} configuration
        on-partitions-assigned
        (fn [consumer topic-partitions]
          (let [assignment-context
                {:event-processor  event-processor
                 :assignments      (kafka-consumer/assignments consumer)
                 :topic-partitions topic-partitions}]
            (log/log-info assignment-context
              "Partitions assigned.")
            (when (and (some? rewind-check) (rewind-required? rewind-check processor))
              (log/log-info assignment-context
                "Rewind required. Seeking to beginning of topic partitions.")
              (kafka-consumer/seek-to-beginning consumer topic-partitions))))
        kafka-consumer-group
        (assoc kafka-consumer-group
          :callbacks {:on-partitions-assigned on-partitions-assigned})]
    (kafka-consumer/with-consumer [kafka-consumer kafka-consumer-group]
      (log/log-info
        {:event-processor event-processor
         :configuration   configuration}
        "Initialising event processor.")
      (every
        (milliseconds interval)
        (try
          (process-events-once (assoc processor :kafka-consumer kafka-consumer))
          (catch Throwable exception
            (log/log-error
              {:event-processor event-processor}
              "Something went wrong in event processor."
              exception)))))))

(defrecord Processor
  [event-processor]
  component/Lifecycle
  (start [component]
    (log/log-info {:event-processor event-processor}
      "Starting event processor.")
    (let [processor (future (process-events-forever component))]
      (assoc component :processor processor)))

  (stop [component]
    (when-let [processor (:processor component)]
      (future-cancel processor))
    (dissoc component :processor)))

(defn new-processor [event-processor]
  (map->Processor {:event-processor event-processor}))