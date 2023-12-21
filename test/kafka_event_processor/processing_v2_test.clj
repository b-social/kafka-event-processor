(ns kafka-event-processor.processing-v2-test
  (:require
    [clojure.test :refer :all]
    [kafka-event-processor.test-support.kafka.combined :as kafka]
    [kafka-event-processor.test-support.postgres.database :as database]
    [kafka-event-processor.test-support.system :as system]
    [kafka-event-processor.test-support.conditional-execution :refer [do-until]]
    [halboy.resource :as hal]
    [kafka-event-processor.utils.generators :as generators]
    [halboy.json :as hal-json]
    [jason.convenience :refer [->wire-json]]
    [kafka-event-processor.test-support.kafka.producer :as producer])
  (:import [kafka_event_processor.processor.protocols EventHandler]))

(let [database (database/new-database)
      {:keys [broker-host
              broker-port] :as kafka} (kafka/new-kafka)
      test-system (-> (merge
                        database
                        {:kafka-configuration
                         {:ssl-keystore-password ""
                          :security-protocol "PLAINTEXT"
                          :ssl-keystore-location ""
                          :ssl-truststore-password ""
                          :value-deserializer-class-config "org.apache.kafka.common.serialization.StringDeserializer"
                          :key-deserializer-class-config "org.apache.kafka.common.serialization.StringDeserializer"
                          :ssl-key-password ""
                          :enable-auto-commit-config "false"
                          :bootstrap-servers (str broker-host ":" broker-port)
                          :ssl-truststore-location ""
                          :auto-offset-reset-config "earliest"}})
                      (system/new-test-system-v2)
                      (atom))]
  (use-fixtures :once
                (database/with-database database)
    (kafka/with-kafka kafka)
    (system/with-system-lifecycle test-system))

  (deftest processing-v2
    (testing "processes events with event handler only"
      (let [events-atom (:atom @test-system)
            ^EventHandler event-handler (:event-handler @test-system)
            event-id (generators/uuid)
            message "I am an event"
            event (-> (hal/new-resource)
                    (hal/add-href :self (str "http://localhost/event/" event-id))
                    (hal/add-property :id event-id)
                    (hal/add-property :message message)
                    (hal/add-property :type :test)
                    hal-json/resource->map
                    ->wire-json)]
        (producer/publish-messages kafka "test"
          [(producer/create-message event)])

        (let [read-events (do-until
                            (fn [] @events-atom)
                            {:matcher #(= 1 (count %))
                             :timeout 60000})
              event (first read-events)]
          (is (= 1 (count read-events)))
          (is (= event-id (:id event)))
          (is (= message (:message event))))

        (let [read-cursors (do-until
                            (fn [] @(.atom event-handler))
                            {:matcher #(= 1 (count %))
                             :timeout 60000})
              cursor (first read-cursors)]
          (is (= 1 (count read-cursors)))
          (is (= event-id (:event-id cursor)))
          (is (= "test" (:topic cursor)))
          (is (= :main (:processor cursor)))
          (is (int? (:partition cursor))))))))
