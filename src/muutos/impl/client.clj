(ns ^:no-doc muutos.impl.client
  (:refer-clojure :exclude [send])
  (:require [clojure.string :as string]
            [cognitect.anomalies :as-alias anomalies]
            [muutos.impl.anomaly :as anomaly :refer [anomaly!]]
            [muutos.impl.base64 :as base64]
            [muutos.impl.connection :as connection]
            [muutos.impl.crypto :as crypto]
            [muutos.impl.sasl :as sasl]
            [muutos.impl.charset :as charset])
  (:import (java.io ByteArrayOutputStream)))

(set! *warn-on-reflection* true)

(defprotocol Client
  (options [this])
  (log [this level event-name data])
  (connection [this])
  (send [this msg])
  (recv [this])
  (oid [this x])
  (aux [this]))

(defn ^:private concat-bytes [& bas]
  (with-open [baos (ByteArrayOutputStream.)]
    (run! (fn [^bytes ba] (.write baos ba)) bas)
    (.toByteArray baos)))

(defn ^:private determine-channel-binding [connection]
  (let [bs (if (connection/secure? connection)
             (concat-bytes
               (charset/bytes "p=tls-server-end-point,,")
               (connection/certificate-hash connection))
             (charset/bytes "n,,"))]
    (base64/encode bs :to :string)))

(defn authentication-flow
  [connection
   {:keys [user password notify-fn]
    :as options}]
  (loop [state options]
    (let [{:keys [type] :as response} (connection/read connection {})]
      (case type
        :error (-> response :ex throw)

        :notice
        (do
          ((or notify-fn log prn) response)
          (recur state))

        :auth/sasl
        (let [client-nonce (sasl/nonce)
              mechanism (response :mechanism)
              client-initial-response {:type :sasl-initial-response
                                       :channel-binding (= "SCRAM-SHA-256-PLUS" mechanism)
                                       :mechanism mechanism
                                       :user user
                                       :nonce client-nonce}]
          (connection/write connection client-initial-response)
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
          (connection/write connection
            {:type :sasl-response
             :client-final-message client-final-message})
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
            (anomaly!
              "Server signature didn't match the expected value"
              ::anomalies/forbidden
              {:expected expected-server-signature
               :actual server-signature}))

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
    (connection/secure connection tls-options)
    (connection/write connection startup-message))

  (authentication-flow connection options))

(def default-options
  {:host "localhost"
   :user "postgres"
   :database "postgres"
   :replication false
   :password "postgres"
   :port 5432
   :key-fn (fn [_table-oid attr-name] attr-name)})

