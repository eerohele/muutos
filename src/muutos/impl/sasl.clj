(ns ^:no-doc muutos.impl.sasl
  "See: https://datatracker.ietf.org/doc/html/rfc5802"
  (:require [cognitect.anomalies :as-alias anomalies]
            [muutos.impl.anomaly :refer [anomaly!]]
            [muutos.impl.base64 :as base64]
            [muutos.impl.digest :as digest]
            [muutos.impl.crypto :as crypto]
            [clojure.string :as string])
  (:import (java.security SecureRandom)))

(set! *warn-on-reflection* true)

(def ^:private ^SecureRandom secure-random (SecureRandom.))

(defn nonce
  "Generate and return a 24-byte nonce (string) for SCRAM-SHA-256
  authentication."
  []
  (let [bytes (byte-array 18)]
    (.nextBytes secure-random bytes)
    (base64/encode bytes :to :string)))

(comment (nonce) ,,,)

(defn parse-server-first-message
  "Given a string containing a RFC5802 server-first-message, return a map that
  contains the message constituents."
  [message]
  (into {:raw-message message}
    (map (fn [s]
           (let [[k v] (string/split s #"=")
                 k (case k
                     "r" :nonce
                     "s" :salt
                     "i" :iteration-count
                     "v" :server-signature
                     (anomaly! "Not implemented" ::anomalies/unsupported {:k k}))
                 v (case k
                     :iteration-count (parse-long v)
                     :salt (base64/decode v)
                     v)]
             [k v])))
    (string/split message #",")))

(comment
  (parse-server-first-message "r=BsNkEzRo7TQWhoZAPGptcxDK/RGuSUtkS03QtqQXXMaONvVM,s=2Jk4WlPzLHPGmZJktB3veQ==,i=4096")
  ,,,)

(defn ^:private xor-bytes
  "Given two byte arrays of equal length, compute their bitwise XOR and return
  the result as a new byte array.

  Each byte in the output array is the XOR of the corresponding bytes in the
  input arrays."
  [^bytes xs ^bytes ys]
  (let [len (alength xs)
        ba (byte-array len)]
    (loop [i 0]
      (if (= i len)
        ba
        (let [x (aget xs i)
              y (aget ys i)]
          (aset ba i (byte (bit-xor x y)))
          (recur (inc i)))))))

(comment
  (xor-bytes (byte-array [1 2 3]) (byte-array [4 5 6]))
  ,,,)

(defn client-proof
  "Given a salted password and RFC5802 AuthMessage (string), return a RFC5802
  ClientProof."
  [salted-password auth-message]
  (let [client-key (crypto/hmac salted-password "Client Key")
        stored-key (digest/sha-256-hash client-key)
        client-signature (crypto/hmac stored-key auth-message)]
    (->
      (xor-bytes client-key client-signature)
      (base64/encode :to :string))))

(defn server-signature
  "Given a salted password and RFC5802 AuthMessage (string), return a RFC5802
  ServerSignature."
  [salted-password auth-message]
  (->
    (crypto/hmac salted-password "Server Key")
    (crypto/hmac auth-message)
    (base64/encode :to :string :padding? false)))
