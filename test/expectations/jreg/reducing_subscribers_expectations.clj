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

(given [call]
  (expect (interaction call)
    (let [chan (channel)
          fiber (ThreadFiber.)
          barrier (CyclicBarrier. 2)]
      (.start fiber)
      (subscribe chan (->eager-reducing-subscriber (last-message-with-earliest :timestamp)
                                                   (interval 20 :millis)
                                                   fiber
                                                   #(do (a-fn1 %) (.await barrier 5 ms))))
      (publish chan {:timestamp 1 :k "foo"}) ; first eager delivery
      (.await barrier 5 ms)
      (.sleep ms 25) ; slightly longer than flush interval
      (publish chan {:timestamp 2 :k "bar"}) ; second eager delivery
      (.await barrier 5 ms)
      (.dispose fiber)))

  (a-fn1 {:timestamp 1 :k "foo"})
  (a-fn1 {:timestamp 2 :k "bar"}))

(given [call]
  (expect (interaction call)
    (let [chan (channel)
          fiber (ThreadFiber.)
          barrier (CyclicBarrier. 2)]
      (.start fiber)
      (subscribe chan (->eager-reducing-subscriber (last-message-with-earliest :timestamp)
                                                   (interval 20 :millis)
                                                   fiber
                                                   #(do (a-fn1 %) (.await barrier 5 ms))))
      (publish chan {:timestamp 1 :k "foo"})
      (.await barrier 5 ms) ; eager delivery
      (publish chan {:timestamp 2 :k "bar"})
      (publish chan {:timestamp 3 :k "baz"})
      (.await barrier 25 ms) ; reduced delivery
      (publish chan {:timestamp 4 :k "qux"})
      (.sleep ms 5) ; to reliably give an overeager subscriber time to erroneously pass that message straight through.
      (publish chan {:timestamp 5 :k "quux"})
      (.await barrier 20 ms))) ; second reduced delivery

  (a-fn1 {:timestamp 1 :k "foo"}) ; eager delivery
  (a-fn1 {:timestamp 2 :k "baz"}) ; reduced delivery
  (a-fn1 {:timestamp 4 :k "quux"})) ; second reduced delivery

(expect (interaction (a-fn1 {:timestamp 2 :k "bar"}))
  (let [chan (channel)
        fiber (ThreadFiber.)
        barrier (CyclicBarrier. 2)]
    (.start fiber)
    (subscribe chan (->eager-reducing-subscriber (last-message-with-earliest :timestamp)
                                                 (interval 50 :millis)
                                                 fiber
                                                 #(do (a-fn1 %) (.await barrier 5 ms))))
    (publish chan {:timestamp 1 :k "foo"})
    (.await barrier 5 ms) ; eager delivery
    (.sleep ms 25)
    (publish chan {:timestamp 2 :k "bar"})
    (.await barrier 30 ms))) ; delivery delayed less than interval, because timer started on the first message, not the second

(given [call]
  (expect (interaction call)
    (let [chan (channel)
          fiber (ThreadFiber.)
          barrier (CyclicBarrier. 2)]
      (.start fiber)
      (subscribe chan (->eager-last-subscriber (interval 20 :millis)
                                               fiber
                                               #(do (a-fn1 %) (.await barrier 5 ms))))
      (publish chan {:k "foo"}) ; first eager delivery
      (.await barrier 5 ms)
      (.sleep ms 25) ; slightly longer than flush interval
      (publish chan {:k "bar"}) ; second eager delivery
      (.await barrier 5 ms)
      (.dispose fiber)))

  (a-fn1 {:k "foo"})
  (a-fn1 {:k "bar"}))

(given [call]
  (expect (interaction call)
    (let [chan (channel)
          fiber (ThreadFiber.)
          barrier (CyclicBarrier. 2)]
      (.start fiber)
      (subscribe chan (->eager-last-subscriber (interval 20 :millis)
                                               fiber
                                               #(do (a-fn1 %) (.await barrier 5 ms))))
      (publish chan {:k "foo"})
      (.await barrier 5 ms) ; eager delivery
      (publish chan {:k "bar"})
      (publish chan {:k "baz"})
      (.await barrier 25 ms) ; delayed delivery
      (publish chan {:k "qux"})
      (.sleep ms 5) ; to reliably give an overeager subscriber time to erroneously pass that message straight through.
      (publish chan {:k "quux"})
      (.await barrier 20 ms))) ; second delayed delivery

  (a-fn1 {:k "foo"}) ; eager delivery
  (a-fn1 {:k "baz"}) ; delayed delivery
  (a-fn1 {:k "quux"})) ; second delayed delivery

(expect (interaction (a-fn1 {:k "bar"}))
  (let [chan (channel)
        fiber (ThreadFiber.)
        barrier (CyclicBarrier. 2)]
    (.start fiber)
    (subscribe chan (->eager-last-subscriber (interval 50 :millis)
                                             fiber
                                             #(do (a-fn1 %) (.await barrier 5 ms))))
    (publish chan {:k "foo"})
    (.await barrier 5 ms) ; eager delivery
    (.sleep ms 25)
    (publish chan {:k "bar"})
    (.await barrier 30 ms))) ; delivery delayed less than interval, because timer started on the first message, not the second

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
