(defproject jreg "0.5.2"
  :description "a Clojure wrapper for Jetlang"
  :jar-name "jreg.jar"
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :dependencies [[org.clojure/clojure "1.5.1"]]
  :deploy-repositories [["releases" :clojars]]
  :profiles {:dev {:dependencies [[expectations "2.0.4"]
                                  [erajure "0.0.4"]
                                  [org.jetlang/jetlang "0.2.10"]]}}
  :plugins [[lein-expectations "0.0.7"]
            [lein-publishers "1.0.11"]]
    :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag"]
                  ["deploy"]
                  ["publish-fig"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]])
