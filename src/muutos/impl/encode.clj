(ns ^:no-doc muutos.impl.encode
  "Encode Clojure maps into java.nio.ByteBuffers for sending to PostgreSQL."
  (:refer-clojure :exclude [flush sync])
  (:require [muutos.impl.charset :as charset])
  (:import (java.nio ByteBuffer)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defmulti encode :type)

(defn ^:private protocol-version
  [^long major ^long minor]
  (+ (bit-shift-left major 16) (bit-shift-left minor 16)))

(defmethod encode :startup
  [{:keys [parameters]}]
  (let [sb (StringBuilder.)]
    (run! (fn [[n v]]
            (.append sb (name n))
            (.append sb \u0000)
            (.append sb (if (ident? v) (name v) (str v)))
            (.append sb \u0000))
      parameters)

    (.append sb \u0000)

    (let [s (str sb)
          bb (charset/byte-buffer s)
          len (+ 4 4 (.remaining bb))]
      (.. (ByteBuffer/allocate len)
        (putInt len)
        (putInt (protocol-version 3 0))
        (put bb)
        (flip)))))

(defmethod encode :sasl-initial-response
  [{:keys [user ^String mechanism nonce channel-binding]}]
  (let [client-first-message (if channel-binding
                               (format "p=tls-server-end-point,,n=%s,r=%s" user nonce)
                               (format "n,,n=%s,r=%s" user nonce))
        bytes-mechanism (charset/byte-buffer mechanism)
        bytes-client-first-message (charset/byte-buffer client-first-message)
        len (+ 4 (.remaining bytes-mechanism) 1 4 (.remaining bytes-client-first-message))]
    (.. (ByteBuffer/allocate (+ 1 len))
      (put (byte 112))
      (putInt len)
      (put bytes-mechanism)
      (put (byte 0))
      (putInt (.remaining bytes-client-first-message))
      (put bytes-client-first-message)
      (flip))))

(defmethod encode :sasl-response
  [{:keys [^String client-final-message]}]
  (let [bb (charset/byte-buffer client-final-message)
        len (+ 4 (.remaining bb))]
    (.. (ByteBuffer/allocate (+ 1 len))
      (put (byte 112))
      (putInt len)
      (put ^ByteBuffer bb)
      (flip))))

(defmethod encode :ssl-request
  [_]
  (.. (ByteBuffer/allocate (+ 4 4))
    (putInt 8)
    (putInt 80877103)
    (flip)))

(defmethod encode :flush
  [_]
  (.. (ByteBuffer/allocate 5)
    (put (byte #_\S 72))
    (putInt 4)
    (flip)))

(def ^ByteBuffer sync
  (.. (ByteBuffer/allocate 5)
    (put (byte #_\H 83))
    (putInt 4)
    (flip)))

(defmethod encode :sync
  [_]
  (.duplicate sync))

(defmethod encode :bind
  [{:keys [^String statement ^String portal parameters]}]
  (let [statement (charset/byte-buffer statement)
        portal (charset/byte-buffer portal)
        parameter-lengths (mapv (fn [^ByteBuffer bb] (or (some-> bb .capacity) 0)) parameters)
        parameter-count (count parameters)
        len (+ 4
              (.remaining portal) 1
              (.remaining statement) 1
              2
              (* 2 parameter-count)
              2
              (* 4 parameter-count)
              (long (reduce + 0 parameter-lengths))
              ;; result columns
              2
              2)
        bb (.. (ByteBuffer/allocate (+ 1 len))
             (put (byte #_\B 66))
             (putInt len)
             (put ^ByteBuffer portal)
             (put (byte 0))
             (put ^ByteBuffer statement)
             (put (byte 0))
             (putShort (short parameter-count)))]

    (dotimes [_ parameter-count]
      (.putShort bb 1))

    (.putShort bb (short parameter-count))

    (run!
      (fn [x]
        (if (nil? x)
          (.putInt bb -1)
          (do
            (.putInt bb (.capacity ^ByteBuffer x))
            (.put bb ^ByteBuffer x))))
      parameters)

    (.putShort bb (short 1))
    (.putShort bb (short 1))

    (doto bb .flip)))

(defmethod encode :execute
  [{:keys [^String portal max-rows]}]
  (let [portal (charset/byte-buffer portal)
        len (+ 4 (.remaining portal) 1 4)]
    (.. (ByteBuffer/allocate (+ 1 len))
      (put (byte #_\E 69))
      (putInt len)
      (put ^ByteBuffer portal)
      (put (byte 0))
      (putInt (int max-rows))
      (flip))))

(defmethod encode :close
  [{:keys [target ^String name]}]
  (let [name (charset/byte-buffer name)
        len (+ 4 1 (.remaining name) 1)]
    (.. (ByteBuffer/allocate (+ 1 len))
      (put (byte #_\C 67))
      (putInt len)
      (put (byte (case target :statement #_\S 83 :portal #_\P 80)))
      (put ^ByteBuffer name)
      (put (byte 0))
      (flip))))

(defmethod encode :close-portal
  [{:keys [^String name]}]
  (let [name (charset/byte-buffer name)
        len (+ 4 1 (.remaining name) 1)]
    (.. (ByteBuffer/allocate (+ 1 len))
      (put (byte #_\C 67))
      (putInt len)
      (put (byte #_\P 80))
      (put ^ByteBuffer name)
      (put (byte 0))
      (flip))))

(defmethod encode :parse
  [{:keys [oids ^String statement ^String query]}]
  (let [statement (charset/byte-buffer statement)
        query (charset/byte-buffer (or query ""))
        param-count (count oids)
        len (+ 4 (.remaining statement) 1 (.remaining query) 1 2 (* 4 param-count))
        bb (.. (ByteBuffer/allocate (+ 1 len))
             (put (byte #_\P 80))
             (putInt len)
             (put ^ByteBuffer statement)
             (put (byte 0))
             (put ^ByteBuffer query)
             (put (byte 0))
             (putShort (short param-count)))]

    (loop [i 0]
      (when (< i param-count)
        (let [oid (nth oids i)]
          (.putInt bb oid))
        (recur (inc i))))

    (doto bb .flip)))

(defmethod encode :describe
  [{:keys [target ^String name]}]
  (let [name (charset/byte-buffer name)
        len (+ 4 1 (.remaining name) 1)]
    (..
      (ByteBuffer/allocate (+ 1 len))
      (put (byte #_\D 68))
      (putInt len)
      (put (byte (case target :statement #_\S 83 :portal #_\P 80)))
      (put ^ByteBuffer name)
      (put (byte 0))
      (flip))))

(def ^ByteBuffer copy-done
  (.. (ByteBuffer/allocate 5)
    (put (byte 99))
    (putInt 4)
    (flip)))

(defmethod encode :copy-done
  [_]
  (.duplicate copy-done))

(defmethod encode :copy-data
  [{:keys [data]}]
  (let [^ByteBuffer bb (encode data)
        len (+ (.remaining bb) 4)]
    (..
      (ByteBuffer/allocate (inc len))
      (put (byte 100))
      (putInt len)
      (put ^ByteBuffer bb)
      (flip))))

(def ^:private ^ByteBuffer buffer-standby-status-update
  (ByteBuffer/allocate (+ 1 64 64 64 64 1)))

(defmethod encode :standby-status-update
  [{:keys [written-lsn flushed-lsn applied-lsn system-clock reply-asap?]}]
  (.. (.duplicate buffer-standby-status-update)
    (put (byte 114))
    (putLong written-lsn)
    (putLong flushed-lsn)
    (putLong applied-lsn)
    (putLong system-clock)
    (put (byte (case reply-asap? true 1 0)))
    (flip)))

(defmethod encode :simple-query
  [{:keys [^String query]}]
  (let [bb (charset/byte-buffer query)
        len (+ 1 4 (.remaining bb) 1)]
    (.. (ByteBuffer/allocate len)
      (put (byte 81))
      (putInt (unchecked-dec-int len))
      (put bb)
      (put (byte 0))
      (flip))))

(defmethod encode :terminate [_]
  (.. (ByteBuffer/allocate 5)
    (put (byte 88))
    (putInt 4)
    (flip)))
