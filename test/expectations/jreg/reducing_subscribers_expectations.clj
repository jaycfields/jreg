(ns expectations.jreg.reducing-subscribers-expectations
  (:use expectations jreg jreg.reducing-subscribers)
  (:import (java.util.concurrent CyclicBarrier CountDownLatch TimeUnit)
           (org.jetlang.fibers ThreadFiber)))

(def ms TimeUnit/MILLISECONDS)

(expect (interaction (a-fn1 {:timestamp 1 :foo "bar"}))
  (let [chan (channel)
        fiber (ThreadFiber.)
        latch (CountDownLatch. 1)]
    (.start fiber)
    (subscribe chan (->simple-reducing-subscriber (last-message-with-earliest :timestamp)
                                                  (interval 5 :millis)
                                                  fiber
                                                  #(do (a-fn1 %) (.countDown latch))))
    (publish chan {:timestamp 1 :foo "bar"})
    (.await latch 10 ms)
    (.dispose fiber)))

(given [call]
  (expect (interaction call)
    (let [chan (channel)
          fiber (ThreadFiber.)
          barrier (CyclicBarrier. 2)]
      (.start fiber)
      (subscribe chan (->simple-reducing-subscriber (last-message-with-earliest :timestamp)
                                                    (interval 5 :millis)
                                                    fiber
                                                    #(do (a-fn1 %) (.await barrier 10 ms))))
      (publish chan {:timestamp 1 :foo "bar"})
      (publish chan {:timestamp 2 :foo "baz"})
      (.await barrier 10 ms)
      (publish chan {:timestamp 3 :foo "quux"})
      (.await barrier 10 ms)))
  (a-fn1 {:timestamp 1 :foo "baz"})
  (a-fn1 {:timestamp 3 :foo "quux"}))

(expect (interaction (a-fn1 {:timestamp 1 :foo "bar"}))
  (let [chan (channel)
        fiber (ThreadFiber.)
        latch (CountDownLatch. 1)]
    (.start fiber)
    (subscribe chan (->eager-reducing-subscriber (last-message-with-earliest :timestamp)
                                                 (interval 5 :secs)
                                                 fiber
                                                 #(do (a-fn1 %) (.countDown latch))))
    (publish chan {:timestamp 1 :foo "bar"})
    (.await latch 10 ms)
    (.dispose fiber)))

(given [call]
  (expect (interaction call)
    (let [chan (channel)
          fiber (ThreadFiber.)
          barrier (CyclicBarrier. 2)]
      (.start fiber)
      (subscribe chan (->eager-reducing-subscriber (last-message-with-earliest :timestamp)
                                                   (interval 30 :millis)
                                                   fiber
                                                   #(do (a-fn1 %) (.await barrier 10 ms))))
      (publish chan {:timestamp 1 :k "foo"})
      (publish chan {:timestamp 2 :k "bar"})
      (publish chan {:timestamp 3 :k "baz"})
      (.await barrier 10 ms) ; first eager delivery
      (.await barrier 40 ms) ; reduced delivery
      (publish chan {:timestamp 4 :k "quux"})
      (.await barrier 10 ms))) ; second eager delivery

  (a-fn1 {:timestamp 1 :k "foo"}) ; first eager delivery
  (a-fn1 {:timestamp 2 :k "baz"}) ; reduced delivery
  (a-fn1 {:timestamp 4 :k "quux"})) ; second eager delivery

(given [call]
  (expect (interaction call)
    (let [chan (channel)
          fiber (ThreadFiber.)
          barrier (CyclicBarrier. 2)]
      (.start fiber)
      (subscribe chan (->eager-reducing-subscriber (last-message-with-earliest :timestamp)
                                                   (interval 100 :millis)
                                                   fiber
                                                   #(do (a-fn1 %) (.await barrier 10 ms))))
      (publish chan {:timestamp 1 :k "foo"})
      (.await barrier 10 ms) ; eager delivery
      (.sleep ms 50)
      (publish chan {:timestamp 2 :k "bar"})
      (.await barrier 60 ms))) ; delayed delivery after less than interval, because the clock started ticking on the first message, not the second

  (a-fn1 {:timestamp 1 :k "foo"}) ; eager delivery
  (a-fn1 {:timestamp 2 :k "bar"})) ; delayed delivery

(expect (interaction (a-fn1 {"bar" {:timestamp 1 :foo "bar" :k "v"}}))
  (let [chan (channel)
        fiber (ThreadFiber.)
        latch (CountDownLatch. 1)]
    (.start fiber)
    (subscribe chan (->keyed-batch-reducing-subscriber
                     :foo
                     (last-message-with-earliest :timestamp)
                     (interval 5 :millis)
                     fiber
                     #(do (a-fn1 %) (.countDown latch))))
    (publish chan {:timestamp 1 :foo "bar" :k "v"})
    (.await latch 10 ms)
    (.dispose fiber)))

(given [call]
  (expect (interaction call)
    (let [chan (channel)
          fiber (ThreadFiber.)
          barrier (CyclicBarrier. 2)]
      (.start fiber)
      (subscribe chan (->keyed-batch-reducing-subscriber
                       :foo
                       (last-message-with-earliest :timestamp)
                       (interval 5 :millis)
                       fiber
                       #(do (a-fn1 %) (.await barrier 10 ms))))
      (publish chan {:timestamp 1 :foo "bar" :k "v1"})
      (publish chan {:timestamp 2 :foo "baz"})
      (publish chan {:timestamp 3 :foo "bar" :k "v2"})
      (.await barrier 10 ms)
      (publish chan {:timestamp 4 :foo "quux"})
      (.await barrier 10 ms)))
  (a-fn1 {"bar" {:timestamp 1 :foo "bar" :k "v2"} "baz" {:timestamp 2 :foo "baz"}})
  (a-fn1 {"quux" {:timestamp 4 :foo "quux"}}))
