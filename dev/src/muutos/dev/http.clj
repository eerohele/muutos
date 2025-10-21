(ns muutos.dev.http
  "A HTTP client (dev-time only)."
  (:require [clojure.core.protocols :refer [Datafiable]]
            [clojure.string :as string]
            [clojure.datafy :as datafy])
  (:import (java.net.http HttpClient HttpClient$Redirect HttpClient$Version HttpRequest HttpRequest$Builder HttpResponse HttpResponse$ResponseInfo HttpRequest$BodyPublishers HttpResponse$BodyHandlers HttpResponse$BodySubscribers)
           (java.net URI)
           (java.time Duration)
           (java.util.concurrent CompletableFuture)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn ^:private persist [m]
  (persistent!
    (reduce-kv
      (fn [m k v]
        (assoc! m k (vec v)))
      (transient {})
      m)))

(extend-protocol Datafiable
  HttpRequest
  (datafy [this]
    (let [timeout (.orElse (.timeout this) nil)
          version (.orElse (.version this) nil)]
      (cond->
        {:expect-continue (.expectContinue this)
         :hash-code (.hashCode this)
         :headers (persist (.map (.headers this)))
         :method (.method this)
         :uri (.uri this)}
        timeout (assoc :timeout timeout)
        version (assoc :version version))))

  HttpResponse
  (datafy [this]
    (let [body (.body this)]
      (cond-> {:status (.statusCode this)
               :headers (persist (.map (.headers this)))
               :version (.version this)
               :uri (.uri this)}
        body (assoc :body body)))))

(defonce ^:private default-http-client
  (..
    (HttpClient/newBuilder)
    (version HttpClient$Version/HTTP_2)
    (followRedirects HttpClient$Redirect/NORMAL)
    (connectTimeout (Duration/ofSeconds 10))
    (build)))

(defn ^:private add-headers [^HttpRequest$Builder request-builder headers]
  (run! (fn [[k v]] (.setHeader request-builder k v)) headers))

(defn request
  ^CompletableFuture
  [& {:keys [http-client method uri headers body-publisher body-handler]
      :or {http-client default-http-client
           body-publisher (HttpRequest$BodyPublishers/noBody)
           body-handler (HttpResponse$BodyHandlers/ofString)
           method :get}}]
  (assert uri)
  (let [method (-> method name string/upper-case)
        request-builder (doto
                          (..
                            (HttpRequest/newBuilder)
                            (method method body-publisher))
                          (add-headers headers))
        request (.. request-builder (uri (URI. uri)) (build))]
    (..
      ^HttpClient http-client
      (sendAsync request body-handler)
      (thenApply
        (fn [response]
          (->
            (datafy/datafy response)
            (with-meta {:request (datafy/datafy request)})))))))

(comment
  "Read Postgres error code descriptions from Postgres data file into a sorted map."

  (def txt
    (->
      (request :uri "https://raw.githubusercontent.com/postgres/postgres/4aa6fa3cd0a2422f1bb47db837b82f2b53d4c065/src/backend/utils/errcodes.txt")
      (deref)
      :body))

  (into (sorted-map)
    (comp
      (remove #(string/starts-with? % "#"))
      (map #(string/split % #"\s+"))
      (filter (comp seq rest))
      (remove #(= "Section:" (first %)))
      (map #(vector (first %) (keyword (string/replace (last %) #"_" "-")))))
    (string/split-lines txt))
  ,,,)
