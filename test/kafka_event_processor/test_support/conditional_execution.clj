(ns kafka-event-processor.test-support.conditional-execution)

(defn passed-stop-time? [stop-time]
  (> (System/currentTimeMillis) stop-time))

(defn do-until
  ([func criteria]
    (let [matcher (:matcher criteria)
          timeout (:timeout criteria 1000)
          stop-time (+ (System/currentTimeMillis) timeout)]
      (do-until func matcher stop-time)))

  ([func matcher stop-time]
    (let [result (func)]
      (cond
        (passed-stop-time? stop-time)
        (throw
          (ex-info "Timed out waiting for matcher" {}))
        (matcher result) result
        :else (do
                (Thread/sleep 1000)
                (do-until func matcher stop-time))))))
