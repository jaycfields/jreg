(set! *warn-on-reflection* true)
(ns expectations.jreg-expectations
  (:import (org.jetlang.core Callback SynchronousDisposingExecutor)
           (org.jetlang.channels MemoryChannel))
  (:use expectations jreg))

(expect Callback (->callback identity))
(expect 1 (let [a (atom 0)]
            (.onMessage (->callback #(swap! % inc)) a)
            @a))

(expect 1
  (let [chan (MemoryChannel.)
        executor (SynchronousDisposingExecutor.)
        a (atom 0)]
    (subscribe chan executor #(swap! % inc))
    (.publish chan a)
    @a))

(expect "ran" (let [a (atom nil)]
                (execute (SynchronousDisposingExecutor.) #(reset! a "ran"))
                @a))