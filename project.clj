(defproject jreg "0.3.0"
  :description "a Clojure wrapper for Jetlang"
  :jar-name "jreg.jar"
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :dependencies [[org.clojure/clojure "1.3.0"]]
  :profiles {:dev {:dependencies [[expectations "1.4.36"]
                                  [org.jetlang/jetlang "0.2.10"]]}}
  :plugins [[lein-expectations "0.0.7"]
            [lein-publishers "1.0.4"]])
