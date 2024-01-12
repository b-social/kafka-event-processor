(ns kafka-event-processor.utils.logging
  (:require
   [cambium.core :as log]))

(defn ^:no-doc get-error-context [context ^Throwable exception]
  (let [exception-class-name (.getCanonicalName (class exception))
        exception-stacktrace (map str (.getStackTrace exception))
        exception-description (str exception)]
    (merge
      {:exception-type        exception-class-name
       :exception-message     (ex-message exception)
       :exception-stacktrace  exception-stacktrace
       :exception-description exception-description
       :exception             exception}
      context)))

(defmacro log-trace
  [context formatted-string]
  `(log/log :trace ~context nil ~formatted-string))

(defmacro log-debug
  [context formatted-string]
  `(log/log :debug ~context nil ~formatted-string))

(defmacro log-info [context formatted-string]
  `(log/log :info ~context nil ~formatted-string))

(defmacro log-warn [context formatted-string]
  `(log/log :warn ~context nil ~formatted-string))

(defmacro log-error
  ([context formatted-string]
   `(log/log :error ~context (:exception ~context) ~formatted-string))
  ([context formatted-string exception]
   `(log-error
      (get-error-context ~context ~exception)
      ~formatted-string)))