(ns jreg
  (:import (java.util.concurrent Executor)
           (org.jetlang.core Callback
                             Disposable)
           (org.jetlang.channels Publisher
                                 Subscriber)))

(defn ->callback ^org.jetlang.core.Callback [f]
  (reify Callback
    (onMessage [_ message]
      (f message))))

(defn subscribe ^org.jetlang.core.Disposable [^Subscriber channel executor f]
  (.subscribe channel executor (->callback f)))

(defn publish [^Publisher channel message]
  (.publish channel message))

(defn execute [^Executor executor runnable]
  (.execute executor runnable))
