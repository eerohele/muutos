(ns muutos.pool
  (:require [muutos.impl.client :as client :refer [Client]]
            [muutos.impl.lockable :as lockable :refer [Lockable with-lock]]
            [muutos.sql-client :as sql])
  (:import (java.lang AutoCloseable)
           (java.util.concurrent LinkedBlockingDeque)
           (java.util.concurrent.atomic AtomicBoolean)
           (java.util.concurrent.locks ReentrantLock)))

(set! *warn-on-reflection* true)

(defprotocol ^:private Pool
  (queue [this])
  (client-opts [this])
  (closed? [this]))

(defn establish [& {:keys [client-opts min-idle-size ^long max-size]
                    :or {min-idle-size 10 max-size 10}}]
  (let [q (LinkedBlockingDeque. max-size)
        -lock (ReentrantLock.)
        -closed? (AtomicBoolean. false)]
    (reify
      AutoCloseable
      (close [_]
        (LinkedBlockingDeque/.forEach q AutoCloseable/.close)
        (LinkedBlockingDeque/.clear q)
        (AtomicBoolean/.set -closed? true))

      Pool
      (queue [_] q)
      (client-opts [_] client-opts)
      (closed? [_] (AtomicBoolean/.get -closed?))

      Lockable
      (lock [_] -lock))))

(defn ^:private acquire [pool]
  (if (closed? pool)
    (throw (ex-info "Pool closed" {}))
    (or
      (LinkedBlockingDeque/.poll (queue pool))
      (with-lock pool
        (let [connection (sql/connect (client-opts pool))]
          (if (LinkedBlockingDeque/.offerLast (queue pool) connection)
            connection
            (AutoCloseable/.close connection)))))))

(defn ^:private return [pool client]
  (if (closed? pool)
    (throw (ex-info "Pool closed" {}))
    (when-not (client/closed? client)
      (when-not (LinkedBlockingDeque/.offerFirst (queue pool) client)
        (AutoCloseable/.close client)))))

(defn borrow [pool]
  (let [client (acquire pool)]
    (reify
      Client
      (options [_] (client/options client))
      (log [_ level event-name data] (client/log client level event-name data))
      (connection [_] (client/connection client))
      (enqueue [_ msg] (client/enqueue client msg))
      (flush [_] (client/flush client))
      (recv [_] (client/recv client))
      (oid [_ x] (client/oid client x))
      (closed? [_] (client/closed? client))

      Lockable
      (lock [_] (lockable/lock client))

      AutoCloseable
      (close [_]
        (return pool client)))))

(comment
  (def pool (establish))
  (def pool (establish :client-opts {:key-fn (fn [_ attr-name] (keyword attr-name))}))

  (with-open [db (borrow pool)]
    (assert (= [{"n" 1}] (sql/eq db ["SELECT $1 AS n" 1]))))

  (AutoCloseable/.close pool)
  ,,,)