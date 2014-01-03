(ns jreg.reducing-subscribers
  (:require [jreg :refer (get-units schedule execute)])
  (:import (org.jetlang.channels Subscribable) (org.jetlang.fibers Fiber)))

(defonce ^:private no-val (Object.))
(defn- val? [v] (not (identical? v no-val)))

(defprotocol ReducingSubscriberState
  (flush-state [s])
  (accept-message [s reduce-fn message]))

(defrecord SimpleReducingSubscriberState [^boolean flush-was-pending pending-val flush-val]
  ReducingSubscriberState
  (flush-state [_] (SimpleReducingSubscriberState. false no-val pending-val))
  (accept-message [_ reduce-fn message]
    (let [flush-was-pending (val? pending-val)]
      (SimpleReducingSubscriberState. flush-was-pending
                                      (if flush-was-pending
                                        (reduce-fn pending-val message)
                                        message)
                                      nil))))

(def ^:private simple-initial-state (SimpleReducingSubscriberState. false no-val nil))

(defrecord KeyedBatchReducingSubscriberState [key-resolver ^boolean flush-was-pending pending-val flush-val]
  ReducingSubscriberState
  (flush-state [_] (KeyedBatchReducingSubscriberState. key-resolver false no-val pending-val))
  (accept-message [_ reduce-fn message]
    (let [flush-was-pending (val? pending-val)]
      (KeyedBatchReducingSubscriberState. key-resolver
                                          flush-was-pending
                                          (let [k (key-resolver message)]
                                            (if (val? pending-val)
                                              (let [pending-k-val (get pending-val k no-val)]
                                                (if (val? pending-k-val)
                                                  (assoc pending-val k (reduce-fn pending-k-val message))
                                                  (assoc pending-val k message)))
                                              {k message}))
                                          nil))))

(defn- keyed-batch-initial-state [key-resolver] (KeyedBatchReducingSubscriberState. key-resolver false no-val nil))

(deftype ReducingSubscriber
    [reduce-fn flush-interval filter-pred ^Fiber fiber f a]
  Runnable
  (run [this]
    (let [state (swap! a flush-state)]
      (f (:flush-val state))))
  Subscribable
  (getQueue [_] fiber)
  (onMessage [this message]
    (when (or (nil? filter-pred) (filter-pred message))
      (let [state (swap! a accept-message reduce-fn message)]
        (when-not (:flush-was-pending state)
          (if (pos? (get-units flush-interval))
            (schedule fiber this flush-interval)
            (execute fiber this)))))))

(defn ->simple-reducing-subscriber
  ([reduce-fn flush-interval fiber f]
     (->simple-reducing-subscriber reduce-fn flush-interval nil fiber f))
  ([reduce-fn flush-interval filter-pred fiber f]
     (->ReducingSubscriber reduce-fn flush-interval filter-pred fiber f (atom simple-initial-state))))

(defn ->keyed-batch-reducing-subscriber
  ([key-resolver reduce-fn flush-interval fiber f]
     (->keyed-batch-reducing-subscriber key-resolver reduce-fn flush-interval nil fiber f))
  ([key-resolver reduce-fn flush-interval filter-pred fiber f]
     (->ReducingSubscriber reduce-fn flush-interval filter-pred fiber f (atom (keyed-batch-initial-state key-resolver)))))

(defn last-message-with-earliest [k]
  (fn [old-val last-message]
    (assoc last-message k (get (or old-val last-message) k))))