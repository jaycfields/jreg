(set! *warn-on-reflection* true)

(ns expectations.jreg-expectations
  (:import (java.util.concurrent CountDownLatch TimeUnit)
           (org.jetlang.core Callback Disposable Scheduler SynchronousDisposingExecutor)
           (org.jetlang.channels MemoryChannel)
           (org.jetlang.fibers ThreadFiber))
  (:use erajure.core expectations jreg))

;; Core

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

(expect "ran" (let [a (atom nil)]
                (execute (SynchronousDisposingExecutor.) #(reset! a "ran"))
                @a))

(set! *warn-on-reflection* false) ; because type-hints on interaction aren't working.

(expect-let [disposable (mock Disposable)]
            (interaction (.dispose disposable))
            (dispose disposable))

(given [expected-call jreg-call]
  (expect-let [scheduler (mock Scheduler)]
    (interaction expected-call)
    jreg-call)

  (.schedule scheduler a-fn 23 TimeUnit/MILLISECONDS)
  (schedule scheduler a-fn 23)

  (.schedule scheduler a-fn 23 TimeUnit/NANOSECONDS)
  (schedule scheduler a-fn (interval 23 :nanos))

  (.schedule scheduler a-fn 23 TimeUnit/MICROSECONDS)
  (schedule scheduler a-fn (interval 23 :micros))

  (.schedule scheduler a-fn 23 TimeUnit/MILLISECONDS)
  (schedule scheduler a-fn (interval 23 :millis))

  (.schedule scheduler a-fn 23 TimeUnit/SECONDS)
  (schedule scheduler a-fn (interval 23 :secs))

  (.schedule scheduler a-fn 23 TimeUnit/MINUTES)
  (schedule scheduler a-fn (interval 23 :mins))

  (.schedule scheduler a-fn 23 TimeUnit/HOURS)
  (schedule scheduler a-fn (interval 23 :hrs))

  (.schedule scheduler a-fn 23 TimeUnit/DAYS)
  (schedule scheduler a-fn (interval 23 :days))

  (.scheduleAtFixedRate scheduler a-fn 5 10 TimeUnit/MILLISECONDS)
  (schedule-at-fixed-rate scheduler a-fn 5 10)

  (.scheduleAtFixedRate scheduler a-fn 5 10 TimeUnit/SECONDS)
  (schedule-at-fixed-rate scheduler a-fn (interval 5 :secs) (interval 10 :secs))

  (.scheduleWithFixedDelay scheduler a-fn 5 10 TimeUnit/MILLISECONDS)
  (schedule-with-fixed-delay scheduler a-fn 5 10)

  (.scheduleWithFixedDelay scheduler a-fn 5 10 TimeUnit/SECONDS)
  (schedule-with-fixed-delay scheduler a-fn (interval 5 :secs) (interval 10 :secs)))

(set! *warn-on-reflection* true)

(defn core-fns-have-return-type-hints []
  (let [cb (->callback nil)] (.onMessage cb nil))
  (let [f (->filter nil)] (.passes f nil))
  (let [d (schedule nil nil nil)] (.dispose d))
  (let [d (schedule-at-fixed-rate nil nil nil nil)] (.dispose d))
  (let [d (schedule-with-fixed-delay nil nil nil nil)] (.dispose d)))

;; Channels

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
          (subscribe chan (->channel-subscriber #(re-find #"good" %)
                                                (SynchronousDisposingExecutor.)
                                                #(swap! received conj %)))
          (publish chan "good message")
          (publish chan "bad message")
          @received))

(given [flush-interval time-unit]
  (expect ["first message" "third message"]
    (let [chan (MemoryChannel.)
          consuming-fiber (ThreadFiber.)
          received (atom [])
          latch (CountDownLatch. 2)]
      (.start consuming-fiber)
      (subscribe chan (->last-subscriber flush-interval
                                         consuming-fiber
                                         (fn [m]
                                           (swap! received conj m)
                                           (.countDown latch))))
      (publish chan "first message")
      (Thread/sleep (+ 2 (.toMillis time-unit 1)))
      (publish chan "second message")
      (publish chan "third message")
      (.await latch 2 time-unit)
      @received))
  1 TimeUnit/MILLISECONDS
  (interval 1 :millis) TimeUnit/MILLISECONDS)

(given [flush-interval time-unit]
  (expect [["first message"] ["second message" "third message"]]
    (let [chan (MemoryChannel.)
          consuming-fiber (ThreadFiber.)
          received (atom [])
          latch (CountDownLatch. 2)]
      (.start consuming-fiber)
      (subscribe chan (->batch-subscriber flush-interval
                                          consuming-fiber
                                          (fn [m]
                                            (swap! received conj m)
                                            (.countDown latch))))
      (publish chan "first message")
      (Thread/sleep (+ 2 (.toMillis time-unit 1)))
      (publish chan "second message")
      (publish chan "third message")
      (.await latch 2 time-unit)
      @received))
  1 TimeUnit/MILLISECONDS
  (interval 1 :millis) TimeUnit/MILLISECONDS)

(given [flush-interval time-unit]
  (expect [{"A" "A 1"}
           {"A" "A 3" "B" "B 1"}]
    (let [chan (MemoryChannel.)
          consuming-fiber (ThreadFiber.)
          received (atom [])
          latch (CountDownLatch. 2)]
      (.start consuming-fiber)
      (subscribe chan (->keyed-batch-subscriber #(.substring ^String % 0 1)
                                                flush-interval
                                                consuming-fiber
                                                (fn [m]
                                                  (swap! received conj m)
                                                  (.countDown latch))))
      (publish chan "A 1")
      (Thread/sleep (+ 2 (.toMillis time-unit 1)))
      (publish chan "A 2")
      (publish chan "A 3")
      (publish chan "B 1")
      (.await latch 2 time-unit)
      @received))
  1 TimeUnit/MILLISECONDS
  (interval 1 :millis) TimeUnit/MILLISECONDS)

(defn channel-fns-have-return-type-hints []
  (let [d (subscribe nil nil)] (.dispose d))
  (let [d (subscribe nil nil nil)] (.dispose d))
  (let [s (->channel-subscriber nil nil)] (.getQueue s))
  (let [s (->channel-subscriber nil nil nil)] (.getQueue s))
  (let [s (->last-subscriber nil nil nil)] (.getQueue s))
  (let [s (->last-subscriber nil nil nil nil)] (.getQueue s)))
