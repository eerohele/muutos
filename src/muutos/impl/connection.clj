(ns ^:no-doc muutos.impl.connection
  (:refer-clojure :exclude [flush read])
  (:require [clojure.string :as string]
            [cognitect.anomalies :as-alias anomalies]
            [muutos.impl.anomaly :as anomaly :refer [anomaly!]]
            [muutos.impl.base64 :as base64]
            [muutos.impl.decode :as decode]
            [muutos.impl.encode :as encode]
            [muutos.impl.charset :as charset]
            [muutos.impl.client :as client]
            [muutos.impl.crypto :as crypto]
            [muutos.impl.lockable :refer [Lockable with-lock]]
            [muutos.impl.sasl :as sasl]
            [muutos.impl.ssl :as ssl])
  (:import (clojure.lang ExceptionInfo) (java.io BufferedInputStream BufferedOutputStream ByteArrayOutputStream InputStream)
           (java.lang AutoCloseable)
           (java.net ConnectException InetSocketAddress Socket SocketException)
           (java.nio ByteBuffer)
           (javax.net.ssl SSLSocket)
           (java.time Duration)
           (java.util.concurrent.atomic AtomicLong AtomicReference)
           (java.util.concurrent.locks ReentrantLock)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def default-options
  {:host "localhost"
   :user "postgres"
   :database "postgres"
   :replication false
   :password "postgres"
   :port 5432
   :key-fn (fn [_table-oid attr-name] attr-name)
   :application_name "Muutos SQL Client"})

(defprotocol Connection
  (closed? [this]
    "Return true iff this connection is closed.")

  (enqueue [this msg]
    "Enqueue a message to be sent to the connected PostgreSQL instance.")

  (secure? [this]
    "Return true iff this connection uses TLS encryption to secure comms with the server.")

  (secure [this options]
    "Upgrade this connection to use TLS encryption if the server requests it.")

  (certificate-hash [this])

  (read [this state]
    "Read a java.nio.ByteBuffer from a socket.")

  (write [this message]
    "Write a java.nio.ByteBuffer into a socket.")

  (flush [this]
    "Flush all buffered writes.")

  (set-last-used! [this nanos]
    "Set the time (in nanoseconds) this connection was last used.")

  (idle [this]
    "Return the time (in nanoseconds) this connection has been idle.")

  (tx-status [this]
    "Get the transaction status of this connection.")

  (alive? [this]
    "Return true iff this connection is alive (able to synchronize with the
    connected PostgreSQL server)."))

(defn ^:private read-n-bytes ^bytes [in n]
  (let [read-bytes (InputStream/.readNBytes in n)]
    (if (= n (alength read-bytes))
      read-bytes
      (anomaly! "EOF reading from socket" ::anomalies/unavailable))))

(defrecord ^:private IO [socket in out])

(defn ^:private io [^Socket socket]
  (let [output-stream (-> socket .getOutputStream BufferedOutputStream.)
        input-stream (-> socket .getInputStream BufferedInputStream.)]
    (IO. socket input-stream output-stream)))

(defn ^:private disconnected? [ex]
  (identical? ::anomalies/unavailable (-> ex ex-data ::anomalies/category)))

(defn ^:private make [^Socket initial-socket]
  ;; Must store socket state in atom to swap it for an SSLSocket.
  (let [state (atom (io initial-socket))
        socket (fn [] (.socket ^IO @state))
        input-stream (fn [] (.in ^IO @state))
        output-stream (fn [] (.out ^IO @state))
        -lock (ReentrantLock.)
        -write (fn [os msg]
                 (let [^ByteBuffer bb (encode/encode msg)
                       bytes (.array bb)
                       offset (+ (.arrayOffset bb) (.position bb))
                       length (.remaining bb)]
                   (BufferedOutputStream/.write os bytes offset length)
                   (.position bb (.limit bb))))
        -last-used (AtomicLong. (System/nanoTime))
        tx-status (AtomicReference. :idle)]
    (reify Connection
      (closed? [_]
        (Socket/.isClosed (socket)))

      (enqueue [this msg]
        (if-not (closed? this)
          (write this msg)
          (anomaly! "Disconnected from server; can't send" ::anomalies/incorrect {:reason :disconnected})))

      (secure [this options]
        (with-lock this
          (write this {:type :ssl-request})
          (flush this)

          (let [prefix (BufferedInputStream/.read (input-stream))]
            ;; \S means yes, so upgrade the connection to use TLS.
            (when (= #_\S 83 prefix)
              (swap! state
                (fn [{:keys [socket]}]
                  (let [ssl-context (ssl/make-context options)
                        ssl-socket (ssl/upgrade-to-ssl-socket ssl-context socket)]
                    (io ssl-socket))))))))

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
          ;; Hand the buffer to the decoder, which turns it into a Clojure map.
          (let [msg (decode/decode buffer state)]
            (when (= :ready-for-query (:type msg))
              (AtomicReference/.set tx-status (:tx-status msg)))
            msg)))

      (write [_ message]
        (-write (output-stream) message))

      (flush [_]
        (BufferedOutputStream/.flush (output-stream)))

      (set-last-used! [_ nanos]
        (AtomicLong/.set -last-used nanos))

      (idle [_]
        (- (System/nanoTime) (AtomicLong/.get -last-used)))

      (tx-status [_]
        (AtomicReference/.get tx-status))

      (alive? [this]
        (try
          (with-lock this
            (enqueue this {:type :sync})
            (flush this)
            (identical? :ready-for-query (-> this (read {}) :type)))
          (catch ExceptionInfo ex
            (if (disconnected? ex)
              false
              (throw ex)))))

      AutoCloseable
      (close [this]
        (swap! state
          (fn [state]
            (try
              (with-lock this
                (let [out (.out ^IO state)]
                  (-write out {:type :terminate})
                  (BufferedOutputStream/.flush out)))
              ;; The socket is already closed when we attempt to send the
              ;; terminate message; NBD.
              (catch SocketException _))
            (Socket/.close (.socket ^IO state))
            state)))

      Lockable
      (lock [_] -lock))))

(defn ^:private concat-bytes [& bas]
  (with-open [baos (ByteArrayOutputStream.)]
    (run! (fn [^bytes ba] (.write baos ba)) bas)
    (.toByteArray baos)))

(defn ^:private determine-channel-binding [connection]
  (let [bs (if (secure? connection)
             (concat-bytes
               (charset/bytes "p=tls-server-end-point,,")
               (certificate-hash connection))
             (charset/bytes "n,,"))]
    (base64/encode bs :to :string)))

(defn authentication-flow
  [connection
   {:keys [user password notify-fn]
    :as options}]
  (loop [state options]
    (let [{:keys [type] :as response} (read connection {})]
      (case type
        :error (-> response :ex throw)

        :notice
        (do
          ((or notify-fn client/log prn) response)
          (recur state))

        :auth/clear-text-password
        (do
          (write connection {:type :password-message
                                        :password password})
          (flush connection)
          (recur state))

        :auth/sasl
        (let [client-nonce (sasl/nonce)
              mechanism (response :mechanism)
              client-initial-response {:type :sasl-initial-response
                                       :channel-binding (= "SCRAM-SHA-256-PLUS" mechanism)
                                       :mechanism mechanism
                                       :user user
                                       :nonce client-nonce}]
          (write connection client-initial-response)
          (flush connection)
          (recur (assoc state :client-nonce client-nonce :auth-mechanism mechanism :user user)))

        :auth/sasl-continue
        (let [salt (response :salt)
              iteration-count (response :iteration-count)
              server-first-message (response :raw-message)
              server-nonce (response :nonce)
              salted-password (crypto/pbkdf2 password salt iteration-count)
              client-first-message-bare (format "n=%s,r=%s" user (state :client-nonce))
              channel-binding (determine-channel-binding connection)
              client-final-message-without-proof (format "c=%s,r=%s" channel-binding server-nonce)
              auth-message (string/join \, [client-first-message-bare
                                            server-first-message
                                            client-final-message-without-proof])
              client-proof (sasl/client-proof salted-password auth-message)
              client-final-message (format "%s,p=%s" client-final-message-without-proof client-proof)]
          (write connection
            {:type :sasl-response
             :client-final-message client-final-message})

          (flush connection)
          (recur (assoc state :salted-password salted-password :auth-message auth-message)))

        :auth/sasl-final
        (let [salted-password (state :salted-password)
              auth-message (state :auth-message)
              expected-server-signature (sasl/server-signature salted-password auth-message)
              server-signature (response :server-signature)]
          (recur (assoc state
                   :expected-server-signature expected-server-signature
                   :server-signature server-signature)))

        :auth/ok
        (let [expected-server-signature (state :expected-server-signature)
              server-signature (state :server-signature)]
          (when-not (= expected-server-signature server-signature)
            (anomaly! "Server signature didn't match the expected value" ::anomalies/forbidden))

          (recur state))

        :parameter
        (let [parameter (response :parameter)]
          (recur (update state :backend-parameters merge parameter)))

        :backend-key-data
        (let [backend-key-data (select-keys response [:pid :secret-key])]
          (recur (update state :backend-key-data merge backend-key-data)))

        :ready-for-query
        (select-keys state [:user :replication :backend-parameters :backend-key-data :auth-mechanism])

        (anomaly! "Not implemented" ::anomalies/unsupported {:type type})))))

(defn start-session
  [connection options]
  (let [tls-options (select-keys options [:key-managers :trust-managers :secure-random])
        startup-parameters (select-keys options [:user :database :options :replication :application_name])
        startup-message {:type :startup :parameters startup-parameters}]
    (with-lock connection
      (secure connection tls-options)
      (write connection startup-message)
      (flush connection)
      (authentication-flow connection options))))

(defn open
  "Given options, open a TCP socket connection to a PostgreSQL server.

  Options:

    :host (string)
      The host name of the PostgreSQL server.

    :port (long)
      The port number the PostgreSQL server is listening on.

    :socket-timeout (java.time.Duration, defauklt: \"PT0S\")
      The SO_TIMEOUT value for the TCP socket.

    :connect-timeout (java.time.Duration, default: \"PT10S\")
      The connect timeout for the TCP socket."
  [& {:keys [host port connect-timeout socket-timeout]
      :or {connect-timeout (Duration/ofSeconds 10)
           socket-timeout (Duration/ofMillis 0)}
      :as options}]
  (let [socket (doto (Socket.)
                 (.setKeepAlive true)
                 (.setTcpNoDelay true)
                 (.setSoTimeout (Duration/.toMillis socket-timeout)))]
    (try
      (let [address (InetSocketAddress. ^String host ^long port)]
        (.connect socket address (Duration/.toMillis connect-timeout))
        (let [connection (make socket)
              session (start-session connection options)]
          (with-meta connection session)))
      (catch ConnectException ex
        (Socket/.close socket)
        (anomaly! "Connection refused" ::anomalies/unavailable {:host host :port port} ex))
      (catch Exception ex
        (Socket/.close socket)
        (anomaly! "Error connecting to PostgreSQL server" ::anomalies/fault {:host host :port port} ex)))))
