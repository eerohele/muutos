(ns ^:no-doc muutos.impl.crypto
  (:require [clojure.string :as string])
  (:import (javax.crypto Mac SecretKeyFactory)
           (javax.crypto.spec PBEKeySpec SecretKeySpec)
           (java.nio.charset StandardCharsets)))

(set! *warn-on-reflection* true)

(defn pbkdf2
  "Given a password (string), salt (byte array), iteration count, and
  derived key length in bits, compute the PBKDF2-HMAC-SHA-256 key derivation
  and return the derived key as a byte array."
  [^String password salt iterations]
  (when (string/blank? password)
    (throw (IllegalArgumentException. "Password can't be an empty string.")))

  (let [spec (PBEKeySpec. (.toCharArray password) salt iterations 256)
        factory (SecretKeyFactory/getInstance "PBKDF2WithHmacSHA256")]
    (.getEncoded (.generateSecret factory spec))))

(defn hmac
  "Given a secret key as a byte array and a message as a string, compute
  the HMAC-SHA-256 digest and return the result as a byte array."
  [key ^String message]
  (let [mac (Mac/getInstance "HmacSHA256")
        key-spec (SecretKeySpec. key "HmacSHA256")]
    (.init mac key-spec)
    (.doFinal mac (.getBytes message StandardCharsets/UTF_8))))
