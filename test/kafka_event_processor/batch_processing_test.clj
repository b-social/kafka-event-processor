(ns kafka-event-processor.batch-processing-test
  (:require
   [clojure.test :refer :all]
   [kafka-event-processor.test-support.kafka.combined :as kafka]
   [kafka-event-processor.test-support.postgres.database :as database]
   [kafka-event-processor.test-support.system :as system]
   [kafka-event-processor.test-support.data :as data]
   [kafka-event-processor.test-support.conditional-execution :refer [do-until]]
   [kafka-event-processor.utils.generators :as generators]
   [kafka-event-processor.test-support.kafka.producer :as producer]))

(defn test-case [& {:keys [resource-type throw-error?]}]
  (let [id (data/random-uuid)
        brn (str "b-social:" resource-type ":" id)
        href (data/random-url)]
    {:resource-type resource-type
     :topic         resource-type
     :id            id
     :brn           brn
     :href          href
     :throw-error?  throw-error?}))

(let [database (database/new-database)
      kafka (kafka/new-kafka)
      test-system (atom (system/new-test-system (merge database kafka)))]
  (use-fixtures :once
    (database/with-database database)
    (kafka/with-kafka kafka)
    (system/with-system-lifecycle test-system))

  (deftest processes-other-topic-events-when-one-topic-fails
    (testing "processes other topic events when one topic fails - last 2 of address-checks are not processed"
      (let [events-atom (:atom @test-system)
            cases [(test-case :resource-type "liveness-checks")
                   (test-case :resource-type "address-checks")
                   (test-case :resource-type "bankruptcy-checks")
                   (test-case :resource-type "liveness-checks")
                   (test-case :resource-type "address-checks" :throw-error? true)
                   (test-case :resource-type "bankruptcy-checks")
                   (test-case :resource-type "liveness-checks")
                   (test-case :resource-type "address-checks")
                   (test-case :resource-type "bankruptcy-checks")]]

        (doseq [{:keys [topic brn throw-error?]} cases]
          (producer/publish-messages kafka topic
            [{:payload {:brn brn :throw-error? throw-error?}}]))

        (let [read-events (do-until
                            (fn [] @events-atom)
                            {:matcher #(= 7 (count %))
                             :timeout 60000})]
          (is (= 7 (count read-events))))))))
