(ns ^:no-doc muutos.impl.digest
  (:import (java.security MessageDigest)))

(set! *warn-on-reflection* true)

(def ^:private ^MessageDigest sha-256
  (MessageDigest/getInstance "SHA-256"))

(defn sha-256-hash
  "Given a byte array, return its SHA-256 digest (bytes)."
  [^bytes bs]
  (.digest sha-256 bs))
