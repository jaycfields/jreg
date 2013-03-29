(set! *warn-on-reflection* true)

(ns expectations.jreg-expectations
  (:import (java.util.concurrent TimeUnit)
           (org.jetlang.core Callback Scheduler SynchronousDisposingExecutor)
           (org.jetlang.channels MemoryChannel))
  (:use erajure.core expectations jreg))

(expect Callback (->callback identity))
(expect 1 (let [a (atom 0)]
            (.onMessage (->callback #(swap! % inc)) a)
            @a))

(expect 1
  (let [chan (MemoryChannel.)
        executor (SynchronousDisposingExecutor.)
        a (atom 0)]
    (subscribe chan executor #(swap! % inc))
    (publish chan a)
    @a))

(expect "ran" (let [a (atom nil)]
                (execute (SynchronousDisposingExecutor.) #(reset! a "ran"))
                @a))

(set! *warn-on-reflection* false) ; because type-hints on interaction aren't working.

(expect-let [scheduler (mock Scheduler)]
            (interaction (.schedule scheduler a-fn 23 TimeUnit/MILLISECONDS))
            (schedule scheduler a-fn 23))

(expect-let [scheduler (mock Scheduler)]
            (interaction (.scheduleAtFixedRate scheduler a-fn 5 10 TimeUnit/MILLISECONDS))
            (schedule-at-fixed-rate scheduler a-fn 5 10))

(expect-let [scheduler (mock Scheduler)]
            (interaction (.scheduleWithFixedDelay scheduler a-fn 5 10 TimeUnit/MILLISECONDS))
            (schedule-with-fixed-delay scheduler a-fn 5 10))
