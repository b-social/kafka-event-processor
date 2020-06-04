(ns kafka-event-processor.utils.generators
  (:import (java.util UUID)))

(defn uuid
  "Generate a new v4 uuid as a string"
  []
  (str (UUID/randomUUID)))
