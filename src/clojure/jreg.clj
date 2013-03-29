(ns jreg
  (:import [org.jetlang.core Callback]))

(defn ->callback [f]
  (reify Callback
    (onMessage [_ message]
      (f message))))

(defn subscribe [channel executor f]
  (.subscribe channel executor (->callback f)))
