(set! *warn-on-reflection* true)

(ns expectations.jreg-expectations
  (:import (java.util.concurrent TimeUnit)
           (org.jetlang.core Callback Disposable Scheduler SynchronousDisposingExecutor)
           (org.jetlang.channels MemoryChannel))
  (:use erajure.core expectations jreg))

(expect 1 (let [a (atom 0)]
            (.onMessage (->callback #(swap! % inc)) a)
            @a))

(given [result message]
       (expect result
               (.passes (->filter #(= "passes" %)) message))
       true "passes"
       false "doesn't pass")

(expect 1
  (let [chan (MemoryChannel.)
        executor (SynchronousDisposingExecutor.)
        a (atom 0)]
    (subscribe chan executor #(swap! % inc))
    (publish chan a)
    @a))

(expect ["good message"]
        (let [chan (MemoryChannel.)
              a (atom [])]
          (subscribe chan (->channel-subscriber (SynchronousDisposingExecutor.)
                                                #(swap! a conj %)
                                                #(re-find #"good" %)))
          (publish chan "good message")
          (publish chan "bad message")
          @a))

(expect "ran" (let [a (atom nil)]
                (execute (SynchronousDisposingExecutor.) #(reset! a "ran"))
                @a))

(set! *warn-on-reflection* false) ; because type-hints on interaction aren't working.

(expect-let [disposable (mock Disposable)]
            (interaction (.dispose disposable))
            (dispose disposable))

(expect-let [scheduler (mock Scheduler)]
            (interaction (.schedule scheduler a-fn 23 TimeUnit/MILLISECONDS))
            (schedule scheduler a-fn 23))

(expect-let [scheduler (mock Scheduler)]
            (interaction (.scheduleAtFixedRate scheduler a-fn 5 10 TimeUnit/MILLISECONDS))
            (schedule-at-fixed-rate scheduler a-fn 5 10))

(expect-let [scheduler (mock Scheduler)]
            (interaction (.scheduleWithFixedDelay scheduler a-fn 5 10 TimeUnit/MILLISECONDS))
            (schedule-with-fixed-delay scheduler a-fn 5 10))
