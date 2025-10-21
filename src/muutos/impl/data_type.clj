(ns ^:no-doc muutos.impl.data-type
  (:require [muutos.codec.bin :as bin]
            [muutos.codec.txt :as txt]))

(def ^:private q-data-type-meta
  "SELECT typtype, typbasetype
   FROM pg_type
   WHERE oid = $1
   LIMIT 1")

(defn install-decoder!
  [query-fn oid]
  (let [{:strs [typtype typbasetype]} (peek (query-fn [q-data-type-meta oid]))]
    (cond
      ;; composite -> record (oid 2249)
      (= \c typtype)
      (do
        (defmethod bin/decode oid [_ bb] (bin/decode (int 2249) bb))
        (defmethod txt/decode oid [_ s] (txt/decode (int 2249) s)))

      ;; enum -> text (oid 25)
      (= \e typtype)
      (do
        (defmethod bin/decode oid [_ bb] (bin/decode (int 25) bb))
        (defmethod txt/decode oid [_ s] (txt/decode (int 25) s)))

      ;; if base type defined (non-zero), use base type decoder
      (pos? typbasetype)
      (do
        (defmethod bin/decode oid [_ bb] (bin/decode (int typbasetype) bb))
        (defmethod txt/decode oid [_ s] (txt/decode (int typbasetype) s))))))
