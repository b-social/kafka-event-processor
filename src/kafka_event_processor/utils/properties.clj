(ns kafka-event-processor.utils.properties
  (:import [java.util Properties]))

(defn ^Properties map->properties
  "Convert a clojure map to a java properties instance"
  [m]
  (reduce
    (fn [^Properties properties [k v]]
      (doto properties
        (.put (name k) v)))
    (Properties.)
    (seq m)))
