(ns kafka-event-processor.processor.component
  (:require [com.stuartsierra.component :as component]
            [kafka-event-processor.utils.logging :as log]
            [kafka-event-processor.processor.consumer :as kafka-consumer]
            [clojure.java.jdbc :as jdbc]
            [kafka-event-processor.utils.generators :as generate]
            [kafka-event-processor.processor.protocols
             :refer [processable? on-event on-complete on-exception rewind-required?]]))

(defn- milliseconds [millis] millis)

(defmacro ^:no-doc every [millis & body]
  `(while (not (Thread/interrupted))
     ~@body
     (Thread/sleep ~millis)))

(defn- process-events-once
  [{:keys [configuration kafka-consumer database event-processor event-handler]
    :as   processor}]
  (log/log-debug
    {:event-processor event-processor
     :assignments     (kafka-consumer/assignments kafka-consumer)}
    "Checking for new event batch.")
  (let [{:keys [timeout]} configuration
        events (kafka-consumer/get-new-events kafka-consumer timeout event-handler)
        event-processing-batch-id (generate/uuid)
        event-processing-batch-context
        {:event-processor           event-processor
         :event-processing-batch-id event-processing-batch-id}]
    (when (pos? (count events))
      (log/log-debug event-processing-batch-context "Starting processing of event batch.")
      (doseq [event events
              :let [event-context
                    {:event-processor           event-processor
                     :event-processing-batch-id event-processing-batch-id}]]
        (try
          (log/log-debug event-context "Starting processing of event.")
          (jdbc/with-db-transaction [transaction (:handle database)]
            (let [database (assoc database :handle transaction)
                  processor (assoc processor :database database)]
              (if (processable? event-handler processor event event-context)
                (try
                  (log/log-debug event-context
                    "Continuing processing of event: not yet processed.")
                  (on-event event-handler processor event event-context)
                  (on-complete event-handler processor event event-context)
                  (log/log-debug event-context "Completed processing of event.")
                  (catch Exception e
                    (log/log-debug event-context "Processing of event caused an exception." e)
                    (on-exception event-handler processor event event-context e)))
                (log/log-debug event-context
                  "Skipping processing of event: already processed."))))
          (catch Throwable exception
            (log/log-error 
              event-context
              "Uncaught exception during processing of event. Retrying."
              exception)
            (kafka-consumer/seek-to-offset kafka-consumer event)
            (throw exception))))
      (kafka-consumer/commit-offset kafka-consumer)
      (log/log-debug event-processing-batch-context "Completed processing of event batch."))))

(defn- process-events-forever
  [{:keys [configuration kafka-consumer-group rewind-check event-processor]
    :as   processor}]
  (let [{:keys [interval]} configuration
        on-partitions-assigned
        (fn [consumer topic-partitions]
          (let [assignment-context
                {:event-processor  event-processor
                 :assignments      (kafka-consumer/assignments consumer)
                 :topic-partitions topic-partitions}]
            (log/log-debug assignment-context "Partitions assigned.")
            (when (and (some? rewind-check) (rewind-required? rewind-check processor))
              (log/log-debug assignment-context
                "Rewind required. Seeking to beginning of topic partitions.")
              (kafka-consumer/seek-to-beginning consumer topic-partitions))))
        kafka-consumer-group
        (assoc kafka-consumer-group
          :callbacks {:on-partitions-assigned on-partitions-assigned})
        processor-as-map (into {} [processor])]
    (kafka-consumer/with-consumer [kafka-consumer kafka-consumer-group]
      (log/log-info
        {:event-processor event-processor
         :configuration   configuration}
        "Initialising event processor.")
      (every
        (milliseconds interval)
        (try
          (process-events-once (assoc processor-as-map :kafka-consumer kafka-consumer))
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

(defn ^:no-doc new-processor
  [event-processor]
  (map->Processor {:event-processor event-processor}))