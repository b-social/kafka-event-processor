(ns kafka-event-processor.processor.protocols)

(defprotocol RewindCheck
  "A handler that is called to define whether the kafka topic needs rewinding"
  :extend-via-metadata true
  (rewind-required? [this processor] "A callback to decide if a rewind is required"))

(defprotocol EventHandler
  "A handler that is called when at certain points in an events lifecycle. The passed processor contains all the configured dependencies."
  :extend-via-metadata true
  (extract-payload [this event] "A callback to transform the event into a suitable format for processing")
  (processable? [this processor event event-context] "A callback to decide if an event should be processed")
  (on-event [this processor event event-context] "A callback for processing an event")
  (on-complete [this processor event event-context] "A callback for when an event has finished processing"))


(defprotocol TwoStageEventHandler
  "A handler that is called when at certain points in an events lifecycle. The passed processor contains all the configured dependencies."
  :extend-via-metadata true
  ;(extract-payload [this event] "A callback to transform the event into a suitable format for processing")
  ;(processable? [this processor event event-context] "A callback to decide if an event should be processed")
  ;(on-event [this processor event event-context] "A callback for storing an event")
  (handle-event [this processor event event-context] "A callback for processing an event")
  (get-unprocessed-events [this processor] "A callback to get all un-processed events")
  ;(on-complete [this processor event event-context] "A callback for when an event has finished processing")
  )
