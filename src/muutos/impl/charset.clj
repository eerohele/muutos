(ns ^:no-doc muutos.impl.charset
  "Convert a string to UTF-8 bytes and back."
  (:refer-clojure :exclude [bytes])
  (:import (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)))

(set! *warn-on-reflection* true)

(defmacro bytes
  "Get the UTF-8 bytes of a string."
  ^bytes [^String s]
  `(String/.getBytes ~s StandardCharsets/UTF_8))

(defmacro byte-buffer
  "Get a UTF-8 java.nio.ByteBuffer of a string."
  ^ByteBuffer [^String s]
  `(.encode StandardCharsets/UTF_8 ~s))

(defmacro string
  "Get the UTF-8 string of a java.nio.ByteBuffer."
  ^String [^ByteBuffer bb]
  `(str (.decode StandardCharsets/UTF_8 ~bb)))
