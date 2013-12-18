(ns jreg
  (:import (java.util.concurrent Executor
                                 TimeUnit)
           (org.jetlang.core Callback
                             Disposable
                             Filter
                             Scheduler
                             SynchronousDisposingExecutor)
           (org.jetlang.channels ChannelSubscription
                                 Converter
                                 KeyedBatchSubscriber
                                 BatchSubscriber
                                 LastSubscriber
                                 MemoryChannel
                                 Publisher
                                 Subscriber)))

(defprotocol ITimeInterval
  (get-units [this])
  (get-time-unit [this]))

(extend-protocol ITimeInterval
  Long
  (get-units [this] this)
  (get-time-unit [_] TimeUnit/MILLISECONDS))

(defrecord TimeInterval [units time-unit]
  ITimeInterval
  (get-units [_] units)
  (get-time-unit [_] time-unit))

(def abbr->time-unit {:nanos TimeUnit/NANOSECONDS
                      :micros TimeUnit/MICROSECONDS
                      :millis TimeUnit/MILLISECONDS
                      :secs TimeUnit/SECONDS
                      :mins TimeUnit/MINUTES
                      :hrs TimeUnit/HOURS
                      :days TimeUnit/DAYS})

(defn interval [units abbr-k]
  (TimeInterval. units (abbr->time-unit abbr-k)))

;; Core

(defn synchronous-disposing-executor [] (SynchronousDisposingExecutor.))

(defn dispose [^Disposable disposable] (.dispose disposable))

(defn ->callback ^org.jetlang.core.Callback [f]
  (reify Callback (onMessage [_ message] (f message))))

(defn ->filter ^org.jetlang.core.Filter [pred]
  (reify Filter (passes [_ message] (boolean (pred message)))))

(defn execute [^Executor executor runnable]
  (.execute executor runnable))

(defn schedule ^org.jetlang.core.Disposable [^Scheduler scheduler command delay]
  (.schedule scheduler command (get-units delay) (get-time-unit delay)))

(defn schedule-at-fixed-rate
  ^org.jetlang.core.Disposable [^Scheduler scheduler command initial-delay period]
  (assert (= (get-time-unit initial-delay) (get-time-unit period)))
  (.scheduleAtFixedRate
   scheduler command (get-units initial-delay) (get-units period) (get-time-unit period)))

(defn schedule-with-fixed-delay
  ^org.jetlang.core.Disposable [^Scheduler scheduler command initial-delay delay]
  (assert (= (get-time-unit initial-delay) (get-time-unit delay)))
  (.scheduleWithFixedDelay
   scheduler command (get-units initial-delay) (get-units delay) (get-time-unit delay)))

;; Channels

(defn channel []
  (MemoryChannel.))

(defn publish [^Publisher channel message]
  (.publish channel message))

(defn subscribe
  (^org.jetlang.core.Disposable [^Subscriber channel executor f]
                                (.subscribe channel executor (->callback f)))
  (^org.jetlang.core.Disposable [^Subscriber channel subscribable]
                                (.subscribe channel subscribable)))

(defn ->channel-subscriber
  (^org.jetlang.channels.Subscribable
   [executor f]
   (ChannelSubscription. executor (->callback f)))
  (^org.jetlang.channels.Subscribable
   [filter-pred executor f]
   (ChannelSubscription. executor (->callback f) (->filter filter-pred))))

(defn ->last-subscriber
  (^org.jetlang.channels.Subscribable
   [flush-interval executor f]
   (LastSubscriber. executor (->callback f)
                    (get-units flush-interval) (get-time-unit flush-interval)))
  (^org.jetlang.channels.Subscribable
   [flush-interval filter-pred executor f]
   (LastSubscriber. executor (->callback f) (->filter filter-pred)
                    (get-units flush-interval) (get-time-unit flush-interval))))

(defn ->converter ^org.jetlang.channels.Converter [f]
  (reify Converter (convert [_ msg] (f msg))))

(defn ->keyed-batch-subscriber
  (^org.jetlang.channels.Subscribable
   [converter flush-interval executor f]
   (KeyedBatchSubscriber. executor (->callback f)
                          (get-units flush-interval) (get-time-unit flush-interval)
                          (->converter converter)))
  (^org.jetlang.channels.Subscribable
   [converter flush-interval filter-pred executor f]
   (KeyedBatchSubscriber. executor (->callback f) (->filter filter-pred)
                          (get-units flush-interval) (get-time-unit flush-interval)
                          (->converter converter))))

(defn ->batch-subscriber
  (^org.jetlang.channels.Subscribable
   [flush-interval executor f]
   (BatchSubscriber. executor (->callback f)
                     (get-units flush-interval) (get-time-unit flush-interval)))
  (^org.jetlang.channels.Subscribable
   [flush-interval filter-pred executor f]
   (BatchSubscriber. executor (->callback f) (->filter filter-pred)
                     (get-units flush-interval) (get-time-unit flush-interval))))
