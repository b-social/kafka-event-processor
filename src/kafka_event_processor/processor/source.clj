(ns kafka-event-processor.processor.source
  (:require
    [clojure.string :as string]

    [halboy.resource :as hal]))

(defn- event-resource->id-from-href
  [event-resource href]
  (let [href (hal/get-href event-resource href)]
    (when-not (nil? href) (last (string/split href #"/")))))

(defn event-resource->id
  "Get the id from the event resource"
  [event-resource]
  (event-resource->id-from-href event-resource :self))

(defn event->topic-and-id
  "Convert the topic and event id into a key"
  [topic event-id]
  (format "%s:%s" topic event-id))
