(set! *warn-on-reflection* true)

(ns expectations.jreg-expectations
  (:import (java.util.concurrent CountDownLatch TimeUnit)
           (org.jetlang.core Callback Disposable Scheduler SynchronousDisposingExecutor)
           (org.jetlang.channels MemoryChannel)
           (org.jetlang.fibers ThreadFiber))
  (:use erajure.core expectations jreg))

(expect 1 (let [a (atom 0)]
            (.onMessage (->callback #(swap! % inc)) a)
            @a))

(given [result filter-pred message]
       (expect result
               (.passes (->filter filter-pred) message))
       true #(= "passes" %) "passes"
       false #(= "passes" %) "doesn't pass"
       true (constantly 1) "anything"
       false (constantly nil) "anything")

(expect 1
  (let [chan (MemoryChannel.)
        executor (SynchronousDisposingExecutor.)
        a (atom 0)]
    (subscribe chan executor #(swap! % inc))
    (publish chan a)
    @a))

(expect ["good message"]
        (let [chan (MemoryChannel.)
              received (atom [])]
          (subscribe chan (->channel-subscriber (SynchronousDisposingExecutor.)
                                                #(swap! received conj %)
                                                #(re-find #"good" %)))
          (publish chan "good message")
          (publish chan "bad message")
          @received))

(expect ["first message" "third message"]
        (let [chan (MemoryChannel.)
              consuming-fiber (ThreadFiber.)
              received (atom [])
              latch (CountDownLatch. 2)]
          (.start consuming-fiber)
          (subscribe chan (->last-subscriber 4 TimeUnit/MILLISECONDS
                                             consuming-fiber
                                             (fn [m]
                                               (swap! received conj m)
                                               (.countDown latch))))
          (publish chan "first message")
          (Thread/sleep 6)
          (publish chan "second message")
          (publish chan "third message")
          (.await latch 5 TimeUnit/MILLISECONDS)
          @received))

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
