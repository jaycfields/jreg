(ns jreg
  (:import (java.util.concurrent Executor
                                 TimeUnit)
           (org.jetlang.core Callback
                             Disposable
                             Filter
                             Scheduler)
           (org.jetlang.channels ChannelSubscription
                                 LastSubscriber
                                 Publisher
                                 Subscriber)))

(defn dispose [^Disposable disposable] (.dispose disposable))

(defn ->callback ^org.jetlang.core.Callback [f]
  (reify Callback (onMessage [_ message] (f message))))

(defn ->filter ^org.jetlang.core.Filter [pred]
  (reify Filter (passes [_ message] (boolean (pred message)))))

(defn ->channel-subscriber
  (^org.jetlang.channels.Subscribable
   [executor f]
   (ChannelSubscription. executor (->callback f)))
  (^org.jetlang.channels.Subscribable
   [executor f filter-pred]
   (ChannelSubscription. executor (->callback f) (->filter filter-pred))))

(defn ->last-subscriber
  (^org.jetlang.channels.Subscribable
   [flush-interval time-unit executor f]
   (LastSubscriber. executor (->callback f) flush-interval time-unit))
  (^org.jetlang.channels.Subscribable
   [flush-interval-millis executor f]
   (->last-subscriber flush-interval-millis TimeUnit/MILLISECONDS executor f)))

(defn subscribe
  (^org.jetlang.core.Disposable [^Subscriber channel executor f]
                                (.subscribe channel executor (->callback f)))
  (^org.jetlang.core.Disposable [^Subscriber channel subscribable]
                                (.subscribe channel subscribable)))

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
