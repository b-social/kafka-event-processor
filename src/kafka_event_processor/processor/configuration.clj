(ns ^:no-doc kafka-event-processor.processor.configuration
  (:require
    [configurati.core
     :refer [define-configuration
             define-configuration-specification
             with-parameter
             with-source
             with-specification
             with-key-fn
             env-source]]
    [configurati.key-fns :refer [remove-prefix]]))

(defn processor-configuration-specification [processor-type]
  (let [prefix (str (name processor-type) "-processor")]
    (letfn [(prefixed-parameter [parameter-name]
              (keyword (str prefix "-" (name parameter-name))))]
      (define-configuration-specification
        (with-key-fn (remove-prefix (keyword prefix)))
        (with-parameter (prefixed-parameter :timeout)
          :type :integer :default 5000)
        (with-parameter (prefixed-parameter :interval)
          :type :integer :default 1000)
        (with-parameter (prefixed-parameter :schema-version)
          :type :integer :default 1)))))

(defn processor-configuration [configuration-prefix processor-type]
  (define-configuration
    (with-source (env-source :prefix configuration-prefix))
    (with-specification
      (processor-configuration-specification processor-type))))

