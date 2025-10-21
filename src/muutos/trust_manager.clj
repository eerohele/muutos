(ns muutos.trust-manager
  "X.509 trust managers (to support encrypted connections)."
  (:require [clojure.java.io :as io])
  (:import (javax.net.ssl TrustManager TrustManagerFactory X509TrustManager)
           (java.security KeyStore)
           (java.security.cert CertificateFactory X509Certificate)))

(set! *warn-on-reflection* true)

(def ^TrustManager credulous-x509-trust-manager
  "An X.509 trust manager that trusts any certificate the server presents to it.

  **Warning**: This trust manager exposes you to man-in-the-middle (MITM) attacks."
  (reify X509TrustManager
    (checkClientTrusted [_this _chain _auth-type])
    (checkServerTrusted [_this _chain _auth-type])
    (getAcceptedIssuers [_this]
      (make-array X509Certificate 0))))

(def ^:private ^CertificateFactory x509-certificate-factory
  (CertificateFactory/getInstance "X.509"))

(def ^:private default-algorithm
  (TrustManagerFactory/getDefaultAlgorithm))

(defn of-rfc-7468-resource
  "Given a `clojure.java.io/IOFactory` (e.g. a `java.io.File` or
  `java.net.URI`) that points to a [X.509 certificate](https://datatracker.ietf.org/doc/html/rfc7468),
  return a collection of X.509 trust managers derived from the certificate.

  You can use this function e.g. to generate a collection of trust managers
  derived from [AWS RDS SSL certificates](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html#UsingWithRDS.SSL.CertificatesDownload)."
  [io-factory]
  (let [key-store (doto
                    (KeyStore/getInstance (KeyStore/getDefaultType))
                    (.load nil))]

    (with-open [input-stream (io/input-stream io-factory)]
      (into []
        (run!
          (fn [^X509Certificate certificate]
            (let [alias (str (random-uuid))]
              (.setCertificateEntry key-store alias certificate)))
          (.generateCertificates x509-certificate-factory input-stream))))

    (let [trust-manager-factory
          (doto
            (TrustManagerFactory/getInstance default-algorithm)
            (.init key-store))]
      (vec (.getTrustManagers trust-manager-factory)))))

(comment
  (of-rfc-7468-resource "/tmp/global-bundle.pem")
  ,,,)
