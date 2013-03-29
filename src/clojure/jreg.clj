(ns jreg
  (:import (org.jetlang.core Callback
                             Disposable)
           (org.jetlang.channels Subscriber)))

(defn ->callback ^Callback [f]
  (reify Callback
    (onMessage [_ message]
      (f message))))

(defn subscribe ^Disposable [^Subscriber channel executor f]
  (.subscribe channel executor (->callback f)))
