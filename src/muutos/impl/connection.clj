(ns ^:no-doc muutos.impl.connection
  (:refer-clojure :exclude [read])
  (:require [cognitect.anomalies :as-alias anomalies]
            [muutos.impl.anomaly :as anomaly :refer [anomaly!]]
            [muutos.impl.decode :as decode]
            [muutos.impl.encode :as encode]
            [muutos.impl.ssl :as ssl])
  (:import (java.io BufferedInputStream BufferedOutputStream InputStream)
           (java.lang AutoCloseable)
           (java.net ConnectException InetSocketAddress Socket SocketException)
           (java.nio ByteBuffer)
           (javax.net.ssl SSLSocket)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defprotocol ^:private Connection
  (closed? [this]
    "Return true iff this connection is closed.")

  (secure? [this]
    "Return true iff this connection uses TLS encryption to secure comms with the server.")

  (secure [this options]
    "Upgrade this connection to use TLS encryption if the server requests it.")

  (certificate-hash [this])

  (read [this state]
    "Read a java.nio.ByteBuffer from a socket.")

  (write [this message]
    "Write a java.nio.ByteBuffer into a socket."))

(defn ^:private read-n-bytes ^bytes [in n]
  (let [read-bytes (InputStream/.readNBytes in n)]
    (if (= n (alength read-bytes))
      read-bytes
      (anomaly! "EOF reading from socket" ::anomalies/unavailable))))

(defn ^:private io [^Socket socket]
  (let [output-stream (-> socket .getOutputStream BufferedOutputStream.)
        input-stream (-> socket .getInputStream BufferedInputStream.)]
    {:socket socket
     :output-stream output-stream
     :input-stream input-stream}))

(defn ^:private make [^Socket initial-socket]
  ;; Must store socket state in atom to swap it for an SSLSocket.
  (let [state (atom (io initial-socket))
        socket (fn [] (-> state deref :socket))
        input-stream (fn [] (-> state deref :input-stream))
        output-stream (fn [] (-> state deref :output-stream))
        -write (fn [os msg]
                 (let [^ByteBuffer bb (encode/encode msg)
                       bytes (.array bb)
                       offset (+ (.arrayOffset bb) (.position bb))
                       length (.remaining bb)]
                   (BufferedOutputStream/.write os bytes offset length)
                   (.position bb (.limit bb))
                   (BufferedOutputStream/.flush ^BufferedOutputStream os)))]
    (reify Connection
      (closed? [_]
        (Socket/.isClosed (socket)))

      (secure [this options]
        (write this {:type :ssl-request})

        (let [prefix (BufferedInputStream/.read (input-stream))]
          ;; \S means yes, so upgrade the connection to use TLS.
          (when (= #_\S 83 prefix)
            (swap! state
              (fn [{:keys [socket]}]
                (let [ssl-context (ssl/make-context options)
                      ssl-socket (ssl/upgrade-to-ssl-socket ssl-context socket)]
                  (io ssl-socket)))))))

      (secure? [_]
        (instance? SSLSocket (socket)))

      (certificate-hash [_]
        (ssl/certificate-hash (socket)))

      (read [_ state]
        (let [in (input-stream)
              ;; Each PostgreSQL message has a header that consists of a
              ;; single-byte prefix and a uint32 that indicates
              ;; the length of the entire message. We'll read those first.
              ;;
              ;; (The length includes the uint32.)
              header (ByteBuffer/wrap (read-n-bytes in 5))
              prefix (.get header)
              ;; Read the number of bytes indicated by the length uint32 (minus
              ;; the length of the uint32 itself).
              len (- (.getInt header) 4)
              ;; Allocate a new byte array to house the prefix byte and the
              ;; body.
              body (ByteBuffer/wrap (read-n-bytes in len))
              buffer (ByteBuffer/allocate (inc len))]
          ;; Make the prefix byte the first byte of the new byte array.
          (ByteBuffer/.put buffer prefix)
          ;; Copy the body into the new byte array.
          (ByteBuffer/.put buffer body)
          ;; Prepare the buffer for reading.
          (.flip buffer)
          ;; Wrap the new byte array into a ByteBuffer and hand it off to the
          ;; decoder, which turns it into a Clojure map.
          (decode/decode buffer state)))

      (write [_ message]
        (-write (output-stream) message))

      AutoCloseable
      (close [_]
        (swap! state
          (fn [{:keys [socket output-stream] :as state}]
            (try
              (-write output-stream {:type :terminate})
              ;; The socket is already closed when we attempt to send the
              ;; terminate message; NBD.
              (catch SocketException _))
            (Socket/.close socket)
            state))))))

(defn open
  "Given a host name (string) and a port number (long), open a TCP socket
  connection to a PostgreSQL server listening on that host and port."
  [host port]
  (try
    (let [address (InetSocketAddress. ^String host ^long port)
          socket (doto (Socket.) (.setKeepAlive true) (.setTcpNoDelay false))]
      (.connect socket address)
      (make socket))
    (catch ConnectException ex
      (anomaly! "Connection refused" ::anomalies/unavailable {:host host :port port} ex))))
