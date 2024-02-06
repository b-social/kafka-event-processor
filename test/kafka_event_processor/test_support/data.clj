(ns kafka-event-processor.test-support.data
  (:require
    [clojure.string :refer [join]]
    [faker.lorem :as lorem]
    [clj-time.core :as time])
  (:import
    [java.util UUID]))

(defn random-uuid []
  (str (UUID/randomUUID)))

(defn random-created-at []
  (time/now))

(def url-template "https://%s.com/%s/%s")

(defn random-url
  ([id]
    (let [words (take 2 (lorem/words))]
      (format url-template (first words) (last words) id)))
  ([]
    (random-url (random-uuid))))
