(ns expectations.jreg.reducing-subscribers-expectations
  (:use expectations jreg jreg.reducing-subscribers)
  (:import (java.util.concurrent CyclicBarrier CountDownLatch TimeUnit)
           (org.jetlang.fibers ThreadFiber)))

(def ms TimeUnit/MILLISECONDS)

(expect [[{:timestamp 1 :foo "bar"}]]
  (side-effects [a-fn1]
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
      (.dispose fiber))))

(expect [[{:timestamp 1 :foo "baz"}] [{:timestamp 3 :foo "quux"}]]
  (side-effects [a-fn1]
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
      (.await barrier 10 ms))))

(expect [[{:sum 1}] [{:sum 8}]]
  (side-effects [a-fn1]
    (let [chan (channel)
          fiber (ThreadFiber.)
          barrier (CyclicBarrier. 2)]
      (.start fiber)
      (subscribe chan (->simple-reducing-subscriber (fn [{:keys [sum]} n] {:sum (+ sum n)})
                                                    {:sum 0}
                                                    (interval 5 :millis)
                                                    odd?
                                                    fiber
                                                    #(do (a-fn1 %) (.await barrier 5 ms))))
      (publish chan 1)
      (publish chan 2) ; ignored because not odd
      (.await barrier 10 ms)
      (publish chan 3)
      (publish chan 4) ; ignored because not odd
      (publish chan 5) ; summed with 3
      (.await barrier 10 ms))))

(expect [[{:timestamp 1 :k "foo"}] [{:timestamp 2 :k "bar"}]]
  (side-effects [a-fn1]
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
      (.dispose fiber))))

(expect [[{:timestamp 1 :k "foo"}] ; eager delivery
         [{:timestamp 2 :k "baz"}] ; reduced delivery
         [{:timestamp 4 :k "quux"}]] ; second reduced delivery
  (side-effects [a-fn1]
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
      (.await barrier 20 ms)))) ; second reduced delivery

(expect [{:timestamp 2 :k "bar"}]
  (in (side-effects [a-fn1]
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
          (.await barrier 30 ms))))) ; delivery delayed less than interval,
                                        ; because timer started on the first message,
                                        ; not the second

(expect [[{:sum 1}] [{:sum 8}]]
  (side-effects [a-fn1]
    (let [chan (channel)
          fiber (ThreadFiber.)
          barrier (CyclicBarrier. 2)]
      (.start fiber)
      (subscribe chan (->eager-reducing-subscriber (fn [{:keys [sum]} n] {:sum (+ sum n)})
                                                    {:sum 0}
                                                    (interval 20 :millis)
                                                    odd?
                                                    fiber
                                                    #(do (a-fn1 %) (.await barrier 5 ms))))
      (publish chan 1)
      (.await barrier 5 ms)
      (publish chan 2) ; ignored because not odd
      (publish chan 3)
      (publish chan 4) ; ignored because not odd
      (publish chan 5) ; summed with 3
      (.await barrier 25 ms))))

(expect [[{:k "foo"}] [{:k "bar"}]]
  (side-effects [a-fn1]
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
      (.dispose fiber))))

(expect [[{:k "foo"}] ; eager delivery
         [{:k "baz"}] ; delayed delivery
         [{:k "quux"}]]
  (side-effects [a-fn1]
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
      (.await barrier 20 ms)))) ; second delayed delivery

(expect [{:k "bar"}]
  (in (side-effects [a-fn1]
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
          (.await barrier 30 ms))))) ; delivery delayed less than interval, because timer started on the first message, not the second

(expect [[{"bar" {:timestamp 1 :foo "bar" :k "v"}}]]
  (side-effects [a-fn1]
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
      (.dispose fiber))))

(expect [[{"bar" {:timestamp 1 :foo "bar" :k "v2"} "baz" {:timestamp 2 :foo "baz"}}]
         [{"quux" {:timestamp 4 :foo "quux"}}]]
  (side-effects [a-fn1]
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
      (.await barrier 10 ms))))

(expect [[{"bar" {:sum 1} "baz" {:sum 3}}]
         [{"bar" {:sum 8}}]]
  (side-effects [a-fn1]
    (let [chan (channel)
          fiber (ThreadFiber.)
          barrier (CyclicBarrier. 2)]
      (.start fiber)
      (subscribe chan (->keyed-batch-reducing-subscriber
                       :foo
                       (fn [{:keys [sum]} {:keys [n]}] {:sum (+ sum n)})
                       {:sum 0}
                       (interval 5 :millis)
                       (comp odd? :n)
                       fiber
                       #(do (a-fn1 %) (.await barrier 10 ms))))
      (publish chan {:foo "bar" :n 1})
      (publish chan {:foo "bar" :n 2})
      (publish chan {:foo "baz" :n 3})
      (.await barrier 10 ms)
      (publish chan {:foo "bar" :n 3})
      (publish chan {:foo "bar" :n 4})
      (publish chan {:foo "bar" :n 5})
      (.await barrier 10 ms))))
