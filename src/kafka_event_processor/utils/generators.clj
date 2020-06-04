(ns kafka-event-processor.utils.generators
  (:import (java.util UUID)))

(defn uuid [] (str (UUID/randomUUID)))
