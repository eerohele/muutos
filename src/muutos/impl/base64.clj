(ns ^:no-doc muutos.impl.base64
  (:import (java.util Base64 Base64$Decoder Base64$Encoder)
           (java.nio.charset StandardCharsets)))

(set! *warn-on-reflection* true)

(def ^:private ^Base64$Encoder encoder
  (Base64/getEncoder))

(def ^:private ^Base64$Decoder decoder
  (Base64/getDecoder))

(defn encode
  "Base64-encode a string or a byte array.

  Options

    :to (:string or :bytes, default: :bytes)
      Return value type.

    :padding? (default: true)
      Whether to retain trailing padding character in encoded data."
  [src & {:keys [to padding?] :or {to :bytes padding? true}}]
  (let [encoder (cond-> encoder (not padding?) (.withoutPadding))
        src (if (bytes? src) src (.getBytes ^String src StandardCharsets/UTF_8))]
    (case to
      :string (.encodeToString encoder src)
      :bytes (.encode encoder ^bytes src))))

(comment
  (encode "x")
  (encode "x" :to :bytes)
  (encode (byte-array [(int \x)]))
  (encode (byte-array [(int \x)]) :to :bytes)
  ,,,)

(defn decode
  "Decode a Base64-encoded string.

  Options:

    :to (:string or :bytes, default: :bytes)
      Return value type."
  [^String s & {:keys [to] :or {to :bytes}}]
  (case to
    :bytes (.decode decoder s)
    :string (String. (.decode decoder s) StandardCharsets/UTF_8)))
