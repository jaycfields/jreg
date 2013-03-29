(ns jreg
  (:import (java.util.concurrent Executor
                                 TimeUnit)
           (org.jetlang.core Callback
                             Disposable
                             Scheduler)
           (org.jetlang.channels Publisher
                                 Subscriber)))

(defn dispose [^Disposable disposable] (.dispose disposable))

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

(defn schedule
  (^Disposable [^Scheduler scheduler command delay time-unit]
               (.schedule scheduler command delay time-unit))
  (^Disposable [scheduler command delay-millis]
               (schedule scheduler command delay-millis TimeUnit/MILLISECONDS)))

(defn schedule-at-fixed-rate
  (^Disposable [^Scheduler scheduler command initial-delay period time-unit]
               (.scheduleAtFixedRate scheduler command initial-delay period time-unit))
  (^Disposable [scheduler command initial-delay-millis period-millis]
               (schedule-at-fixed-rate scheduler command initial-delay-millis period-millis
                                       TimeUnit/MILLISECONDS)))

(defn schedule-with-fixed-delay
  (^Disposable [^Scheduler scheduler command initial-delay delay time-unit]
               (.scheduleWithFixedDelay scheduler command initial-delay delay time-unit))
  (^Disposable [scheduler command initial-delay-millis delay-millis]
               (schedule-with-fixed-delay scheduler command initial-delay-millis delay-millis
                 TimeUnit/MILLISECONDS)))
