(ns ^:no-doc muutos.impl.bb
  (:import (java.nio ByteBuffer)))

(set! *warn-on-reflection* true)

;; Aliases for easier referring to https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html.
(defmacro int8
  "Read a byte (int8) from a ByteBuffer."
  [bb]
  (let [bb (vary-meta bb assoc :tag `ByteBuffer)]
    `(.get ~bb)))

(defmacro int16
  "Read a short (int16) from a ByteBuffer."
  [bb]
  (let [bb (vary-meta bb assoc :tag `ByteBuffer)]
    `(.getShort ~bb)))

(defmacro int32
  "Read an int (int32) from a ByteBuffer."
  [bb]
  (let [bb (vary-meta bb assoc :tag `ByteBuffer)]
    `(.getInt ~bb)))

(defmacro int64
  "Read a long (int64) from a ByteBuffer."
  [bb]
  (let [bb (vary-meta bb assoc :tag `ByteBuffer)]
    `(.getLong ~bb)))

(defmacro float4
  "Read a double (float4) from a ByteBuffer."
  [bb]
  (let [bb (vary-meta bb assoc :tag `ByteBuffer)]
    `(.getFloat ~bb)))

(defmacro float8
  "Read a double (float8) from a ByteBuffer."
  [bb]
  (let [bb (vary-meta bb assoc :tag `ByteBuffer)]
    `(.getDouble ~bb)))

(defn slice
  "Given a length and a ByteBuffer, return a slice of the given length from the
  ByteBuffer.

  If length is negative, returns nil.

  Advances the position of the original ByteBuffer by the length of the slice."
  ^ByteBuffer [^long len ^ByteBuffer bb]
  (when (nat-int? len)
    (let [slice (.. bb (slice) (limit len))
          pos (.position bb)]
      (.position bb (+ pos len))
      slice)))
