(ns kafka-event-processor.custom-event-parsing-test
  (:require
   [clojure.test :refer :all]
   [jason.convenience :as json]
   [kafka-event-processor.test-support.kafka.combined :as kafka]
   [kafka-event-processor.test-support.postgres.database :as database]
   [kafka-event-processor.test-support.system :as system]
   [kafka-event-processor.test-support.conditional-execution :refer [do-until]]
   [kafka-event-processor.utils.generators :as generators]
   [kafka-event-processor.test-support.kafka.producer :as producer]
   [kafka-event-processor.processor.protocols :refer [EventHandler ExtractPayloadFromRecord]]))

(deftype AtomEventHandler
         [atom]
  EventHandler
  (extract-payload
    [this event]
    (throw (ex-info "Called extract-payload in error" {:this this
                                                       :event event})))
  (processable?
    [this processor event event-context]
    true)
  (on-event
    [this processor event _]
    (swap! (:atom processor) conj event))
  (on-complete
    [this processor {:keys [topic partition payload]} {:keys [event-processor]}]
    (swap! atom conj {:processor event-processor
                      :topic     topic
                      :partition partition
                      :payload payload}))

  ExtractPayloadFromRecord
  (extract-from-record [_handler record]
    (let [header (->
                   record
                   (.headers)
                   (.lastHeader "h1"))
          parsed (->
                   record
                   (.value)
                   (json/<-wire-json))
          header-key (keyword (.key header))
          header-value (String. (.value header))]
      (assoc parsed
        header-key header-value))))

(let [database (database/new-database)
      kafka (kafka/new-kafka)
      test-system (atom (system/new-test-system
                          (assoc
                            (merge database kafka)
                            :event-handler (AtomEventHandler. (atom [])))))]
  (use-fixtures :once
    (database/with-database database)
    (kafka/with-kafka kafka)
    (system/with-system-lifecycle test-system))

  (deftest processing
    (testing "processes events with event handler only"
      (let [events-atom (:atom @test-system)
            event-handler (:event-handler @test-system)
            event-id (generators/uuid)
            header-value "I'm a header for an event"
            message "I am an event"
            event {:id event-id
                   :message message
                   :type :test}]

        (producer/publish-messages kafka "test"
          [{:headers {:h1 header-value}
            :payload event}])

        (let [read-events (do-until
                            (fn [] @events-atom)
                            {:matcher #(= 1 (count %))
                             :timeout 60000})
              event (first read-events)
              event-properties (:payload event)]
          (is (= 1 (count read-events)))
          (is (= event-id (:id event-properties)))
          (is (= message (:message event-properties)))
          (is (= (:topic event) "test"))
          (is (= header-value (:h1 event-properties))))

        (let [read-cursors (do-until
                             (fn [] @(.atom event-handler))
                             {:matcher #(= 1 (count %))
                              :timeout 60000})
              cursor (first read-cursors)]
          (is (= 1 (count read-cursors)))
          (is (= event-id (get-in cursor [:payload :id])))
          (is (= "test" (:topic cursor)))
          (is (= :main (:processor cursor)))
          (is (int? (:partition cursor))))))))
