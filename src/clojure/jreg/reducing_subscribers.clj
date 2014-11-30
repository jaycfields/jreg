(ns jreg.reducing-subscribers
  (:require [jreg :refer (dispose get-units schedule schedule-at-fixed-rate execute)])
  (:import (org.jetlang.channels Subscribable) (org.jetlang.fibers Fiber)))

(defonce ^:private no-val (Object.))
(defn- val? [v] (not (identical? v no-val)))

(defprotocol SubscriberStateFlush
  (flush-state [s]))
(defprotocol ReducingSubscriberStateAccept
  (accept-message [s reduce-fn init-val message]))
(defprotocol EagerReducingSubscriberStateAccept
  (eager-accept [s reduce-fn init-val message fiber flush-runnable flush-interval])
  (rollback-from-accept [s]))

(defrecord SimpleReducingSubscriberState [^boolean flush-was-pending pending-val flush-val]
  SubscriberStateFlush
  (flush-state [_] (SimpleReducingSubscriberState. false no-val pending-val))
  ReducingSubscriberStateAccept
  (accept-message [_ reduce-fn init-val message]
    (let [flush-was-pending (val? pending-val)]
      (SimpleReducingSubscriberState. flush-was-pending
                                      (if flush-was-pending
                                        (reduce-fn pending-val message)
                                        (if init-val
                                          (reduce-fn init-val message)
                                          message))
                                      nil))))

(def ^:private simple-initial-state (SimpleReducingSubscriberState. false no-val nil))

(defrecord KeyedBatchReducingSubscriberState [key-resolver ^boolean flush-was-pending pending-val flush-val]
  SubscriberStateFlush
  (flush-state [_] (KeyedBatchReducingSubscriberState. key-resolver false no-val pending-val))
  ReducingSubscriberStateAccept
  (accept-message [_ reduce-fn init-val message]
    (let [flush-was-pending (val? pending-val)]
      (KeyedBatchReducingSubscriberState. key-resolver
                                          flush-was-pending
                                          (let [k (key-resolver message)]
                                            (if (val? pending-val)
                                              (let [pending-k-val (get pending-val k no-val)]
                                                (assoc pending-val k
                                                       (if (val? pending-k-val)
                                                         (reduce-fn pending-k-val message)
                                                         (if init-val
                                                           (reduce-fn init-val message)
                                                           message))))
                                              {k (if init-val
                                                   (reduce-fn init-val message)
                                                   message)}))
                                          nil))))

(defn- keyed-batch-initial-state [key-resolver] (KeyedBatchReducingSubscriberState. key-resolver false no-val nil))

(deftype ReducingSubscriber
    [reduce-fn init-val flush-interval filter-pred ^Fiber fiber f a]
  Runnable
  (run [_]
    (let [state (swap! a flush-state)]
      (f (:flush-val state))))
  Subscribable
  (getQueue [_] fiber)
  (onMessage [this message]
    (when (or (nil? filter-pred) (filter-pred message))
      (let [state (swap! a accept-message reduce-fn init-val message)]
        (when-not (:flush-was-pending state)
          (if (pos? (get-units flush-interval))
            (schedule fiber this flush-interval)
            (execute fiber this)))))))

(defrecord EagerReducingSubscriberState [pending-val schedule-control do-flush-val do-dispose ^boolean do-execute]
  SubscriberStateFlush
  (flush-state [_]
    (let [keep-on-schedule? (val? pending-val)]
      (EagerReducingSubscriberState. no-val (if keep-on-schedule? schedule-control nil)
                                     pending-val (if keep-on-schedule? nil schedule-control)
                                     false)))
  EagerReducingSubscriberStateAccept
  (eager-accept [_ reduce-fn init-val message fiber flush-runnable flush-interval]
    (let [new-schedule-control (if schedule-control
                                 nil
                                 (schedule-at-fixed-rate fiber flush-runnable flush-interval flush-interval))]
      (EagerReducingSubscriberState. (if (val? pending-val)
                                       (reduce-fn pending-val message)
                                       (if init-val
                                         (reduce-fn init-val message)
                                         message))
                                     (or schedule-control new-schedule-control)
                                     nil nil (boolean new-schedule-control))))
  (rollback-from-accept [_]
    (when do-execute (dispose schedule-control))))

(def ^:private eager-initial-state (map->EagerReducingSubscriberState {:pending-val no-val :do-execute false}))

(defrecord EagerLastSubscriberState [pending-val schedule-control do-flush-val do-dispose ^boolean do-execute]
  SubscriberStateFlush
  (flush-state [_]
    (let [keep-on-schedule? (val? pending-val)]
      (EagerLastSubscriberState. no-val
                                 (if keep-on-schedule? schedule-control nil)
                                 pending-val
                                 (if keep-on-schedule? nil schedule-control)
                                 false)))
  EagerReducingSubscriberStateAccept
  (eager-accept [_ _ _ message fiber flush-runnable flush-interval]
    (let [new-schedule-control (if schedule-control
                                 nil
                                 (schedule-at-fixed-rate fiber flush-runnable flush-interval flush-interval))]
      (EagerLastSubscriberState. message
                                 (or schedule-control new-schedule-control)
                                 nil
                                 nil
                                 (boolean new-schedule-control))))
  (rollback-from-accept [_]
    (when do-execute (dispose schedule-control))))

(def ^:private eager-last-subscriber-initial-state (map->EagerLastSubscriberState {:pending-val no-val :do-execute false}))

(defn- swap-with-rollback! [atom rollback-f f u v w x y z]
  (loop [oldval @atom]
    (let [newval (f oldval u v w x y z)]
      (if (compare-and-set! atom oldval newval)
        newval
        (do
          (rollback-f newval)
          (recur @atom))))))

(deftype EagerReducingSubscriber [reduce-fn init-val flush-interval filter-pred ^Fiber fiber cb a]
  Runnable
  (run [_]
    (let [state (swap! a flush-state)
          v (:do-flush-val state)
          schedule-control (:do-dispose state)]
      (when (val? v) (cb v))
      (when schedule-control (dispose schedule-control))))
  Subscribable
  (getQueue [_] fiber)
  (onMessage [this message]
    (when (or (nil? filter-pred) (filter-pred message))
      (let [state (swap-with-rollback! a rollback-from-accept
                                       eager-accept reduce-fn init-val message fiber this flush-interval)]
        (when (:do-execute state)
          (execute fiber this))))))

(defn ->simple-reducing-subscriber
  ([reduce-fn flush-interval fiber f]
     (->simple-reducing-subscriber reduce-fn flush-interval nil fiber f))
  ([reduce-fn flush-interval filter-pred fiber f]
     (->simple-reducing-subscriber reduce-fn nil flush-interval filter-pred fiber f))
  ([reduce-fn init-val flush-interval filter-pred fiber f]
     (->ReducingSubscriber reduce-fn init-val flush-interval filter-pred fiber f (atom simple-initial-state))))

(defn ->eager-reducing-subscriber
  ([reduce-fn flush-interval fiber f]
     (->eager-reducing-subscriber reduce-fn flush-interval nil fiber f))
  ([reduce-fn flush-interval filter-pred fiber f]
     (->eager-reducing-subscriber reduce-fn nil flush-interval nil fiber f))
  ([reduce-fn init-val flush-interval filter-pred fiber f]
     (when-not (pos? (get-units flush-interval))
       (throw (IllegalArgumentException. "flush-interval must be positive")))
     (->EagerReducingSubscriber reduce-fn init-val flush-interval filter-pred fiber f (atom eager-initial-state))))

(defn ->eager-last-subscriber
  "Logically equivalent to an eager-reducing-subscriber with a reduce-fn of
   (fn [_ m] m) but optimized a bit."
  ([flush-interval fiber f] (->eager-last-subscriber flush-interval nil fiber f))
  ([flush-interval filter-pred fiber f]
     (when-not (pos? (get-units flush-interval))
       (throw (IllegalArgumentException. "Just use jreg/->last-subscriber with a zero flush-interval, silly!")))
     (->EagerReducingSubscriber nil nil flush-interval filter-pred fiber f (atom eager-last-subscriber-initial-state))))

(defn ->keyed-batch-reducing-subscriber
  ([key-resolver reduce-fn flush-interval fiber f]
     (->keyed-batch-reducing-subscriber key-resolver reduce-fn flush-interval nil fiber f))
  ([key-resolver reduce-fn flush-interval filter-pred fiber f]
     (->keyed-batch-reducing-subscriber key-resolver reduce-fn nil flush-interval filter-pred fiber f))
  ([key-resolver reduce-fn init-val flush-interval filter-pred fiber f]
     (->ReducingSubscriber reduce-fn init-val flush-interval filter-pred fiber f (atom (keyed-batch-initial-state key-resolver)))))

(defn last-message-with-earliest [k]
  (fn [old-val last-message]
    (assoc last-message k (get (or old-val last-message) k))))
