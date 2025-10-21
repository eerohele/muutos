(ns ^:no-doc muutos.impl.ssl
  (:require [muutos.impl.digest :as digest])
  (:import (java.net InetAddress Socket)
           (java.security.cert Certificate)
           (javax.net.ssl KeyManager SSLContext SSLSocket SSLSocketFactory TrustManager)))

(set! *warn-on-reflection* true)

(def ^:private tls-context
  (SSLContext/getInstance "TLS"))

(defn make-context
  [{:keys [key-managers trust-managers secure-random]}]
  (doto tls-context
    (SSLContext/.init
      (when key-managers (into-array KeyManager key-managers))
      (when trust-managers (into-array TrustManager trust-managers))
      secure-random)))

(def ^:private protocols ["TLSv1.3" "TLSv1.2"])

(defn upgrade-to-ssl-socket
  "Given a javax.net.ssl.SSLContext and a java.net.Socket, upgrade the Socket to
  a SSLSocket."
  [ssl-context ^Socket socket]
  (let [ssl-socket-factory (SSLContext/.getSocketFactory ssl-context)
        ssl-socket (SSLSocketFactory/.createSocket ssl-socket-factory
                     socket
                     (-> socket Socket/.getInetAddress InetAddress/.getHostName)
                     (-> socket Socket/.getPort)
                     true)]
    (doto ssl-socket
      (SSLSocket/.setEnabledProtocols (into-array String protocols))
      (SSLSocket/.startHandshake))))

(defn ^:private end-entity-certificate [^SSLSocket socket]
  (-> socket .getSession .getPeerCertificates ^Certificate (aget 0) .getEncoded))

(defn certificate-hash
  "Given a javax.net.ssl.SSLSocket, return the SHA-256 hash (bytes) of the end-
  entity certificate of the peer the socket is connected to."
  [^SSLSocket socket]
  (-> socket end-entity-certificate digest/sha-256-hash))
