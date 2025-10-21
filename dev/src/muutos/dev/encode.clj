(ns muutos.dev.encode
  (:require [clojure.set :as set]
            [muutos.codec.bin :as bin]
            [muutos.impl.charset :as charset])
  (:import (java.nio ByteBuffer)
           (java.time Instant)))

(set! *warn-on-reflection* true)

(defn ^:private encode-instant ^ByteBuffer [^Instant inst]
  (bin/encode inst))

(defmulti encode :type)

(defmethod encode :begin [{:keys [lsn commit-timestamp xid]}]
  (.. (ByteBuffer/allocate (+ 1 8 8 4))
    (put (byte #_\B 66))
    (putLong lsn)
    (put (encode-instant commit-timestamp))
    (putInt xid)
    (flip)))

(defn ^:private encode-attributes ^ByteBuffer [attributes]
  (let [size (count attributes)
        strlens (transduce (map (fn [{:keys [^String name]}]
                                  (inc (.remaining (charset/byte-buffer name)))))
                  + 0 attributes)
        ^ByteBuffer bb (.. (ByteBuffer/allocate (+ 4 (* size 1 4 4) strlens))
                         (putShort size))]
    (run!
      (fn [{:keys [flags ^String name data-type-oid type-modifier]}]
        (.put bb (byte (case flags #{:key} 1 0)))
        (.put bb (charset/byte-buffer name))
        (.put bb (byte 0))
        (.putInt bb data-type-oid)
        (.putInt bb type-modifier))
      attributes)
    (doto bb .flip)))

(defmethod encode :relation
  [{:keys [xid oid ^String namespace ^String relation replica-identity attributes]}]
  (let [attributes (encode-attributes attributes)
        namespace (charset/byte-buffer namespace)
        relation (charset/byte-buffer relation)]
    (-> (ByteBuffer/allocate (+ 1 4 (if xid 4 0) (.remaining namespace) 1 (.remaining relation) 1 1 (.remaining attributes)))
      (.put (byte #_\R 82))
      (cond-> xid (.putInt xid))
      (.putInt oid)
      (.put namespace)
      (.put (byte 0))
      (.put relation)
      (.put (byte 0))
      (.put (byte (case replica-identity
                   :primary-key 100
                   :nothing 110
                   :full 102
                   :index 105)))
      (.put attributes)
      (.flip))))

(defmethod encode :commit [{:keys [commit-lsn tx-end-lsn commit-timestamp]}]
  (.. (ByteBuffer/allocate (+ 1 1 8 8 8))
    (put (byte #_\C 67))
    (put (byte 0))
    (putLong commit-lsn)
    (putLong tx-end-lsn)
    (put (encode-instant commit-timestamp))
    (flip)))

(defmethod encode :message [{:keys [xid flags lsn ^String prefix ^bytes content]}]
  (let [len (alength content)
        prefix (charset/byte-buffer prefix)]
    (-> (ByteBuffer/allocate (+ 1 (if xid 4 0) 1 8 (.remaining prefix) 1 4 len))
      (.put (byte #_\M 77))
      (cond-> xid (.putInt xid))
      (.put (byte (case flags :none 0 :transactional 1)))
      (.putLong lsn)
      (.put prefix)
      (.put (byte 0))
      (.putInt len)
      (.put content)
      (.flip))))

(defmethod encode :truncate [{:keys [xid oids parameters]}]
  (let [bb (-> (ByteBuffer/allocate (+ 1 (if xid 4 0) 4 1 (* (count oids) 4)))
             (.put (byte #_\T 84))
             (cond-> xid (.putInt xid))
             (.putInt (int (count oids)))
             (.put (byte (cond
                           (set/subset? #{:cascade :restart-identity} parameters) 3
                           (set/subset? #{:restart-identity} parameters) 2
                           (set/subset? #{:cascade} parameters) 1
                           :else 0))))]
    (run! (fn [oid] (.putInt bb oid)) oids)
    (doto bb .flip)))

(defmethod encode :origin [{:keys [commit-lsn ^String name]}]
  (let [name (charset/byte-buffer name)]
    (.. (ByteBuffer/allocate (+ 1 8 (.remaining name) 1))
      (put (byte #_\O 79))
      (putLong commit-lsn)
      (put name)
      (put (byte 0))
      (flip))))

(defn length [x]
  (cond
    (nil? x) 1
    (= :unchanged-toasted-value x) 1
    (string? x) (+ 4 (alength (charset/bytes ^String x)))
    :else (+ 4 (.remaining ^ByteBuffer x))))

(defn ^:private encode-tuple ^ByteBuffer [tuple]
  (let [cnt (count tuple)
        len (transduce (map length) + 0 tuple)
        bb (ByteBuffer/allocate (+ 4 (* cnt 1) len))]
    (.putShort bb cnt)
    (run!
      (fn [x]
        (cond
          (nil? x)
          (.put bb (byte #_\n 110))

          (= :unchanged-toasted-value x)
          (.put bb (byte #_\u 117))

          (string? x)
          (do
            (.put bb (byte #_\t 116))
            (.putInt bb (alength (charset/bytes ^String x)))
            (.put bb (charset/byte-buffer ^String x)))

          :else
          (do
            (.put bb (byte #_\b 98))
            (.putInt bb (.remaining ^ByteBuffer x))
            (.put bb ^ByteBuffer x))))
      tuple)
    (doto bb .flip)))

(defmethod encode :insert [{:keys [xid oid new-tuple]}]
  (let [tuple-bb (encode-tuple new-tuple)
        bb (-> (ByteBuffer/allocate (+ 1 (if xid 4 0) 4 1 (.remaining tuple-bb)))
             (.put (byte #_\I 73))
             (cond-> xid (.putInt xid))
             (.putInt oid)
             (.put (byte #_\N 78)))]
    (.put bb tuple-bb)
    (doto bb .flip)))

(defmethod encode :update [{:keys [xid oid replica-identity old-tuple new-tuple] :as message}]
  (let [old-tuple? (contains? message :old-tuple)
        old-tuple-bb (when old-tuple? (encode-tuple old-tuple))
        new-tuple-bb (encode-tuple new-tuple)
        bb (-> (ByteBuffer/allocate
                 (+ 1
                   (if xid 4 0)
                   4
                   (if replica-identity 1 0)
                   (.remaining new-tuple-bb)
                   1
                   (if old-tuple? (.remaining old-tuple-bb) 0)))
             (.put (byte #_\U 85))
             (cond-> xid (.putInt xid))
             (.putInt oid))]
    (when (and old-tuple? replica-identity)
      (case replica-identity
        :primary-key (.put bb (byte #_\K 75))
        :full (.put bb (byte #_\O 79))
        nil)
      (.put bb old-tuple-bb))
    (.put bb (byte #_\N 78))
    (.put bb new-tuple-bb)
    (doto bb .flip)))

(defmethod encode :delete [{:keys [xid oid replica-identity old-tuple]}]
  (let [tuple-bb (encode-tuple old-tuple)
        bb (-> (ByteBuffer/allocate (+ 1 4 (if xid 4 0) (if replica-identity 1 0) (.remaining tuple-bb)))
             (.put (byte #_\D 68))
             (cond-> xid (.putInt xid))
             (.putInt oid))]
    (when (= :primary-key replica-identity) (.put bb (byte #_\K 75)))
    (when (= :full replica-identity) (.put bb (byte #_\O 79)))
    (.put bb tuple-bb)
    (doto bb .flip)))

(defmethod encode :type [{:keys [xid oid ^String namespace ^String name]}]
  (let [namespace (charset/byte-buffer namespace)
        name (charset/byte-buffer name)]
    (-> (ByteBuffer/allocate (+ 1 4 (if xid 4 0) (.remaining namespace) 1 (.remaining name) 1))
      (.put (byte #_\Y 89))
      (cond-> xid (.putInt xid))
      (.putInt oid)
      (.put namespace)
      (.put (byte 0))
      (.put name)
      (.put (byte 0))
      (.flip))))

(defmethod encode :stream-start [{:keys [xid segment]}]
  (-> (ByteBuffer/allocate (+ 1 4 1))
    (.put (byte #_\S 83))
    (.putInt xid)
    (.put (case segment :first (byte 1) (byte 0)))
    (.flip)))

(defmethod encode :stream-stop [& _]
  (-> (ByteBuffer/allocate 1)
    (.put (byte #_\E 69))
    (.flip)))

(defmethod encode :stream-commit [{:keys [xid commit-lsn tx-end-lsn commit-timestamp]}]
  (.. (ByteBuffer/allocate (+ 1 4 1 8 8 8))
    (put (byte #_\c 99))
    (putInt xid)
    (put (byte 0))
    (putLong commit-lsn)
    (putLong tx-end-lsn)
    (put (encode-instant commit-timestamp))
    (flip)))

(defmethod encode :stream-abort [{:keys [xid subtransaction-xid abort-lsn tx-timestamp]}]
  (-> (ByteBuffer/allocate (+ 1 4 4 (if abort-lsn 8 0) (if tx-timestamp 8 0)))
    (.put (byte #_\A 65))
    (.putInt xid)
    (.putInt subtransaction-xid)
    (cond-> abort-lsn (.putLong abort-lsn))
    (cond-> tx-timestamp (.put (encode-instant tx-timestamp)))
    (.flip)))
