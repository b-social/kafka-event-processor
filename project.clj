(defproject b-social/kafka-event-processor "0.1.2"

  :description "A library for processing kafka events as a stuartsierra component and configured with configurati. Opinionated that the event processing will be wrapped in a jdbc transaction."

  :url "https://github.com/b-social/kafka-event-processor"

  :license {:name "The MIT License"
            :url  "https://opensource.org/licenses/MIT"}

  :dependencies [[org.clojure/clojure "1.10.0"]
                 [com.stuartsierra/component "1.0.0"]
                 [io.logicblocks/configurati "0.5.2"]
                 [org.apache.kafka/kafka-clients "2.3.0"]
                 [cambium/cambium.core "0.9.3"]
                 [cambium/cambium.codec-cheshire "0.9.3"]
                 [org.clojure/java.jdbc "0.7.11"]]

  :plugins [[lein-cloverage "1.1.2"]
            [lein-shell "0.5.0"]
            [lein-ancient "0.6.15"]
            [lein-changelog "0.3.2"]
            [lein-eftest "0.5.9"]
            [lein-codox "0.10.7"]
            [lein-cljfmt "0.6.7"]
            [lein-kibit "0.1.8"]
            [lein-bikeshed "0.5.2"]]

  :profiles {:test {:dependencies
                      [[eftest "0.5.9"]
                       [freeport "1.0.0"]
                       [com.opentable.components/otj-pg-embedded "0.13.3"]
                       [org.apache.kafka/kafka_2.12 "2.3.0"
                        :exclusions [org.apache.zookeeper/zookeeper
                                     org.slf4j/slf4j-log4j12]]
                       [org.apache.curator/curator-test "5.0.0"]
                       [com.impossibl.pgjdbc-ng/pgjdbc-ng "0.8.4"]
                       [hikari-cp "2.12.0"]
                       [b-social/jason "0.1.5"]
                       [b-social/vent "0.6.5"]
                       [halboy "5.1.0"]]
                    :eftest {:multithread? false}}}

  :cloverage
  {:ns-exclude-regex [#"^user"]}

  :bikeshed {:max-line-length 100}

  :codox
  {:namespaces  [#"^kafka-event-processor\."]
   :metadata    {:doc/format :markdown}
   :output-path "docs"
   :doc-paths   ["docs"]
   :source-uri  "https://github.com/b-social/kafka-event-processor/blob/{version}/{filepath}#L{line}"}

  :cljfmt {:indents ^:replace {#".*" [[:inner 0]]}}

  :deploy-repositories
  {"releases" {:url "https://repo.clojars.org" :creds :gpg}}

  :release-tasks
  [["shell" "git" "diff" "--exit-code"]
   ["change" "version" "leiningen.release/bump-version" "release"]
   ["codox"]
   ["changelog" "release"]
   ["shell" "sed" "-E" "-i" "" "s/\"[0-9]+\\.[0-9]+\\.[0-9]+\"/\"${:version}\"/g" "README.md"]
   ["shell" "git" "add" "."]
   ["vcs" "commit"]
   ["vcs" "tag"]
   ["deploy"]
   ["change" "version" "leiningen.release/bump-version"]
   ["vcs" "commit"]
   ["vcs" "tag"]
   ["vcs" "push"]]

  :aliases {"test"      ["with-profile" "test" "eftest" ":all"]
            "precommit" ["do"
                         ["check"]
                         ["kibit" "--replace"]
                         ["cljfmt" "fix"]
                         ["with-profile" "test" "bikeshed"
                          "--name-collisions" "false"
                          "--verbose" "true"]
                         ["test"]]})
