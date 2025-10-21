(ns muutos.dev.backend
  (:refer-clojure :exclude [send])
  (:import (java.io OutputStream)
           (java.lang AutoCloseable)
           (java.net ServerSocket Socket)
           (java.nio ByteBuffer)
           (java.nio.channels Channels ReadableByteChannel WritableByteChannel)))

(set! *warn-on-reflection* true)

(defprotocol Server
  (send [this message])
  (recv [this]))

(defn authentication-ok ^ByteBuffer []
  (.. (ByteBuffer/allocate (+ 1 8 1))
    (put (byte \R))
    (putInt 8)
    (putInt 0)
    (flip)))

(defn ready-for-query ^ByteBuffer []
  (.. (ByteBuffer/allocate (+ 1 4 1))
    (put (byte \Z))
    (putInt 5)
    (put (byte \I))
    (flip)))

;; TODO
(defn row-description ^ByteBuffer []
  (.. (ByteBuffer/allocate (+ 1 4 2))
    (put (byte \T))
    (putInt 6)
    (putShort 0)
    (flip)))

(defn command-complete ^ByteBuffer []
  (.. (ByteBuffer/allocate (+ 1 4 8))
    (put (byte \C))
    (putInt (+ 4 8))
    (put (.getBytes "SELECT 0" "UTF-8"))
    (flip)))

(defn authenticate [send]
  (send (authentication-ok))
  (send (ready-for-query)))

(defn -send
  [out-stream out-chan msg]
  (WritableByteChannel/.write out-chan msg)
  (OutputStream/.flush out-stream))

(defn read-ssl-request [^ReadableByteChannel chan]
  (let [bb (ByteBuffer/allocate 8)]
    (.read chan bb)
    (.flip bb)
    (let [_len (.getInt bb)
          code (.getInt bb)]
      {:code code})))

(defn read-startup-message [^ReadableByteChannel chan]
  (let [bb (ByteBuffer/allocate 4)]
    (.read chan bb)
    (.flip bb)
    (let [len (.getInt bb)
          bb (ByteBuffer/allocate len)]
      (.read chan bb)
      (.flip bb))))

(defn start ^AutoCloseable [& {:keys [accept port] :or {accept authenticate}}]
  (let [server-socket (ServerSocket. port)

        state (future
                (try
                  (let [socket (.accept server-socket)
                        out-stream (.getOutputStream socket)
                        in-stream (.getInputStream socket)
                        in-chan (Channels/newChannel in-stream)
                        out-chan (Channels/newChannel out-stream)]
                    (read-ssl-request in-chan)
                    (.write out-stream (byte \N))
                    (.flush out-stream)
                    (read-startup-message in-chan)
                    (accept (partial -send out-stream out-chan))
                    {:socket socket
                     :in-stream in-stream
                     :out-stream out-stream
                     :out-chan out-chan})
                  (catch Exception ex
                    (println :ex ex))))]
    (reify
      Server
      (send [_ message]
        (when-not (some-> ^Socket (:socket @state) .isClosed)
          (-send (:out-stream @state) (:out-chan @state) message)))

      (recv [_]
        #_TODO)

      AutoCloseable
      (close [_]
        (some-> ^Socket (:socket @state) .close)
        (.close server-socket)))))

(comment
  (def server (start :port 5432))

  #_(.close server)

  ;; TODO:
  ;; - Close connection before writing anything
  ;; - Close connection in the middle of writing header (first 5 bytes)
  ;; - Close connection after writing header
  ;; - Close connection in the middle of writing body
  ;; - Close connection after writing body
  ;; - Close connection after writing entire message

  (require '[muutos.sql-client :as pg])

  (def server (start :port 5432))
  (.close server)

  (def f (pg/connect))
  f


  (send server authentication-ok)
  (send server ready-for-query)

  ;; Only send prefix
  (send server
    (.. (ByteBuffer/allocate 1)
      (put (byte \R))
      (flip)))

  (.close server)

  (send server
    (.. (ByteBuffer/allocate (+ 1 8 1))
      (put (byte \R))
      (putInt 8)
      (flip)))
  f
  (.close server)

  f
  (.pid (java.lang.ProcessHandle/current))
  ,,,)

