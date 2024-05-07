(ns kafka-event-processor.processor.protocols
  (:require 
   [com.kroo.service-base.kafka-event-processor.protocols :as p]))

;; re-export protocols to ease transition

(def RewindCheck p/RewindCheck)
(def rewind-required? p/rewind-required?)

(def ExtractPayloadFromRecord p/ExtractPayloadFromRecord)
(def extract-from-record p/extract-from-record)

(def EventHandler p/EventHandler)
(def extract-payload p/extract-payload)
(def processable? p/processable?)
(def on-event p/on-event)
(def on-complete p/on-complete)


