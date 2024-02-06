(ns kafka-event-processor.processing-test
  (:require
   [clojure.test :refer :all]
   [kafka-event-processor.test-support.kafka.combined :as kafka]
   [kafka-event-processor.test-support.postgres.database :as database]
   [kafka-event-processor.test-support.system :as system]
   [kafka-event-processor.test-support.data :as data]
   [kafka-event-processor.test-support.conditional-execution :refer [do-until]]
   [kafka-event-processor.utils.generators :as generators]
   [kafka-event-processor.test-support.kafka.producer :as producer])
  (:import [kafka_event_processor.processor.protocols EventHandler]))

(defn test-case [& {:keys [resource-type]}]
  (let [id (data/random-uuid)
        brn (str "b-social:" resource-type ":" id)
        href (data/random-url)]
    {:resource-type resource-type
     :topic         resource-type
     :id            id
     :brn           brn
     :href          href}))

(let [database (database/new-database)
      kafka (kafka/new-kafka)
      test-system (atom (system/new-test-system (merge database kafka)))]
  (use-fixtures :once
    (database/with-database database)
    (kafka/with-kafka kafka)
    (system/with-system-lifecycle test-system))

  (deftest processing
    (testing "processes events with event handler only"
      (let [events-atom (:atom @test-system)
            ^EventHandler event-handler (:event-handler @test-system)
            event-id (generators/uuid)
            message "I am an event"
            event {:id event-id
                   :message message
                   :type :test}]

        (producer/publish-messages kafka "test"
          [{:payload event}])

        (let [read-events (do-until
                            (fn [] @events-atom)
                            {:matcher #(= 1 (count %))
                             :timeout 60000})
              event (first read-events)
              event-properties (:payload event)]
          (is (= 1 (count read-events)))
          (is (= event-id (:id event-properties)))
          (is (= message (:message event-properties)))
          (is (= (:topic event) "test")))

        (let [read-cursors (do-until
                             (fn [] @(.atom event-handler))
                             {:matcher #(= 1 (count %))
                              :timeout 60000})
              cursor (first read-cursors)]
          (is (= 1 (count read-cursors)))
          (is (= event-id (get-in cursor [:payload :id])))
          (is (= "test" (:topic cursor)))
          (is (= :main (:processor cursor)))
          (is (int? (:partition cursor)))))))


  (deftest processes-other-topic-events-when-one-topic-fails
    (let [events-atom (:atom @test-system)
          ^EventHandler event-handler (:event-handler @test-system)
          cases [(test-case :resource-type "liveness-checks")
                 (test-case :resource-type "address-checks")
                 (test-case :resource-type "bankruptcy-checks")
                 (test-case :resource-type "liveness-checks")
                 (test-case :resource-type "address-checks")
                 (test-case :resource-type "bankruptcy-checks")
                 (test-case :resource-type "liveness-checks")
                 (test-case :resource-type "address-checks")
                 (test-case :resource-type "bankruptcy-checks")]]

      (doseq [{:keys [topic brn]} cases]
        (producer/publish-messages kafka topic
          [{:payload {:brn brn}}]))

      #_(let [read-events (do-until
                          (fn [] @events-atom)
                          {:matcher #(= 9 (count %))
                           :timeout 60000})
            event (first read-events)]
        (is (= 9 (count read-events))))

      ))
  )
