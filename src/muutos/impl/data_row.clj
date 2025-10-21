(ns ^:no-doc muutos.impl.data-row
  "Parse PostgreSQL tuples into Clojure/Java data."
  (:require [cognitect.anomalies :as-alias anomalies]
            [muutos.codec.bin :as bin]
            [muutos.codec.txt :as txt]
            [muutos.impl.data-type :as data-type]
            [muutos.error :as-alias error]
            [muutos.impl.charset :as charset])
  (:import (clojure.lang ExceptionInfo PersistentArrayMap PersistentHashMap)
           (java.lang Iterable)
           (java.nio ByteBuffer)
           (java.util Iterator)))

(set! *warn-on-reflection* true)

(defn ^:private empty-map [size]
  (if (<= size 8)
    PersistentArrayMap/EMPTY
    PersistentHashMap/EMPTY))

(defn ^:private decode-bin
  [query-fn data-type-oid ^ByteBuffer bb attempts]
  (let [original-pos (.position bb)]
    (try
      (bin/decode data-type-oid bb)
      (catch ExceptionInfo ex
        (if (and query-fn (zero? attempts) (= ::error/unknown-data-type (-> ex ex-data :kind)))
          (do
            (.position bb original-pos)
            (data-type/install-decoder! query-fn (-> ex ex-data :oid))
            (decode-bin query-fn data-type-oid bb (inc attempts)))
          (throw ex))))))

(defn ^:private decode-txt
  [query-fn data-type-oid ^String s attempts]
  (try
    (txt/decode data-type-oid s)
    (catch ExceptionInfo ex
      (if (and query-fn (zero? attempts) (= ::error/unknown-data-type (-> ex ex-data :kind)))
        (do
          (data-type/install-decoder! query-fn (-> ex ex-data :oid))
          (decode-txt query-fn data-type-oid s (inc attempts)))
        (throw ex)))))

(defn parse
  [attrs tuple &
   {:keys [key-fn format query-fn]
    :or {format :txt}}]
  (let [^Iterator attr-defs-it (.iterator ^Iterable attrs)
        ^Iterator vals-it (.iterator ^Iterable tuple)]
    (loop [m (-> attrs count empty-map transient)]
      (if (and (.hasNext attr-defs-it) (.hasNext vals-it))
        (let [n-attr (.next attr-defs-it)
              n-val (cond-> (.next vals-it) (identical? format :txt) (some-> charset/string))
              attr-name (key-fn (n-attr :table-oid) (n-attr :name))
              data-type-oid (n-attr :data-type-oid)
              decode (case format
                       :txt (fn [s] (decode-txt query-fn data-type-oid s 0))
                       :bin (fn [bb] (decode-bin query-fn data-type-oid bb 0))
                       :unknown identity)
              attr-val (cond
                         (nil? n-val) nil
                         (keyword? n-val) n-val #_:unchanged-toasted-value
                         :else (decode n-val))]
          (recur (cond-> m (some? attr-val) (assoc! attr-name attr-val))))
        (persistent! m)))))
