(defproject jreg "0.5.1"
  :description "a Clojure wrapper for Jetlang"
  :jar-name "jreg.jar"
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :dependencies [[org.clojure/clojure "1.5.1"]]
  :profiles {:dev {:dependencies [[expectations "2.0.1"]
                                  [erajure "0.0.4"]
                                  [org.jetlang/jetlang "0.2.10"]]}}
  :plugins [[lein-expectations "0.0.7"]
            [lein-publishers "1.0.11"]])
