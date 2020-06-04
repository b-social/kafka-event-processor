(ns kafka-event-processor.processor.source
  (:require
    [clojure.string :as string]

    [halboy.resource :as hal]))

(defn event-resource->id-from-href [event-resource href]
  (let [href (hal/get-href event-resource href)]
    (when-not (nil? href) (last (string/split href #"/")))))

(defn event-resource->id [event-resource]
  (event-resource->id-from-href event-resource :self))

(def event->topic :topic)
(def event->partition :partition)
(def event->event-resource :resource)
(def event->id (comp event-resource->id event->event-resource))

(defn event->topic-and-id [topic event-id]
  (format "%s:%s" topic event-id))
