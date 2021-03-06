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

(expect true (.passes (->filter #(= "passes" %)) "passes"))
(expect false (.passes (->filter #(= "passes" %)) "doesn't pass"))
(expect true (.passes (->filter (constantly 1)) "anything"))
(expect false (.passes (->filter (constantly nil)) "anything"))

(expect "ran" (let [a (atom nil)]
                (execute (synchronous-disposing-executor) #(reset! a "ran"))
                @a))

(expect #(verify % (.dispose))
  (doto (mock Disposable)
    (dispose)))

(expect #(verify % (.schedule a-fn1 23 TimeUnit/MILLISECONDS))
  (doto (mock Scheduler)
    (schedule a-fn1 23)))

(expect true (from-each [[kw enum] {:nanos TimeUnit/NANOSECONDS
                                 :micros TimeUnit/MICROSECONDS
                                 :millis TimeUnit/MILLISECONDS
                                 :secs TimeUnit/SECONDS
                                 :mins TimeUnit/MINUTES
                                 :hrs TimeUnit/HOURS
                                 :days TimeUnit/DAYS}]
            (let [m (mock Scheduler)]
              (schedule m a-fn1 (interval 23 kw))
              (verify m (.schedule a-fn1 23 enum)))))

(expect #(verify % (.scheduleAtFixedRate a-fn1 5 10 TimeUnit/MILLISECONDS))
  (doto (mock Scheduler)
    (schedule-at-fixed-rate a-fn1 5 10)))

(expect #(verify % (.scheduleAtFixedRate a-fn1 5 10 TimeUnit/SECONDS))
  (doto (mock Scheduler)
    (schedule-at-fixed-rate a-fn1 (interval 5 :secs) (interval 10 :secs))))

(expect #(verify % (.scheduleWithFixedDelay a-fn1 5 10 TimeUnit/MILLISECONDS))
  (doto (mock Scheduler)
    (schedule-with-fixed-delay a-fn1 5 10)))

(expect #(verify % (.scheduleWithFixedDelay a-fn1 5 10 TimeUnit/SECONDS))
  (doto (mock Scheduler)
    (schedule-with-fixed-delay a-fn1 (interval 5 :secs) (interval 10 :secs))))

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

(expect ["first message" "third message"]
  (from-each [[flush-interval time-unit] {1 TimeUnit/MILLISECONDS
                                          (interval 1 :millis) TimeUnit/MILLISECONDS}]
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
      @received)))

(expect [["first message"] ["second message" "third message"]]
  (from-each [[flush-interval time-unit] {1 TimeUnit/MILLISECONDS
                                          (interval 1 :millis) TimeUnit/MILLISECONDS}]
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
      @received)))

(expect [{"A" "A 1"}
         {"A" "A 3" "B" "B 1"}]
  (from-each [[flush-interval time-unit] {1 TimeUnit/MILLISECONDS
                                          (interval 1 :millis) TimeUnit/MILLISECONDS}]
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
      @received)))

(defn channel-fns-have-return-type-hints []
  (let [d (subscribe nil nil)] (.dispose d))
  (let [d (subscribe nil nil nil)] (.dispose d))
  (let [s (->channel-subscriber nil nil)] (.getQueue s))
  (let [s (->channel-subscriber nil nil nil)] (.getQueue s))
  (let [s (->last-subscriber nil nil nil)] (.getQueue s))
  (let [s (->last-subscriber nil nil nil nil)] (.getQueue s))
  (let [s (->batch-subscriber nil nil nil)] (.getQueue s))
  (let [s (->batch-subscriber nil nil nil nil)] (.getQueue s)))
