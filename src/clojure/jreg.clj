(ns jreg
  (:import (java.util.concurrent Executor)
           (org.jetlang.core Callback
                             Disposable)
           (org.jetlang.channels Subscriber)))

(defn ->callback ^Callback [f]
  (reify Callback
    (onMessage [_ message]
      (f message))))

(defn subscribe ^Disposable [^Subscriber channel executor f]
  (.subscribe channel executor (->callback f)))

(defn execute [^Executor executor runnable]
  (.execute executor runnable))
