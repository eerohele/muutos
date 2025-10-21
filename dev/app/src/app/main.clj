(ns app.main
  (:gen-class)
  (:require [clojure.pprint :as pprint]
            [com.brunobonacci.mulog :as μ]
            [muutos.sql-client :as sql]
            [muutos.subscriber :as subscriber])
  (:import (java.nio.charset StandardCharsets)))

(set! *warn-on-reflection* true)

(defonce ^{:private true :doc "Source database connection."} pg-src
  (delay (sql/connect :host "postgres.postgres.svc.cluster.local")))

(comment (eq @pg-src ["SELECT $1" 1]) ,,,)

(defn ^:private log
  [level event data]
  (μ/log event :level level :data data))

(def ^:private replication-slot-name "my_slot")

(defn ^:private handler
  ([{:keys [type] :as msg}]
   (log :info ::wal-data
     (cond-> msg
       (= :message type)
       ;; For this demo app, we'll assume every logical replication message
       ;; carries a string payload.
       (update :content
         (fn [^bytes content] (String. content StandardCharsets/UTF_8))))))
  ([msg ack]
   (handler msg)
   (ack)))

(defn -main [& _args]
  (μ/start-publisher! {:type :console
                       :transform (fn [events]
                                    (remove (comp #{:trace :debug} :level) events))})

  ;; Create replication slot if it doesn't yet exist.
  (sql/ignoring-dupes (sql/create-slot @pg-src replication-slot-name))

  (sql/ignoring-dupes (sql/eq @pg-src ["CREATE PUBLICATION p FOR ALL TABLES"]))

  ;; Dereference to subscriber to have it block until the runtime is shut down
  ;; or until the subscriber encounters an error.
  @(subscriber/connect replication-slot-name
     :publications #{"p"}
     :host "postgres.postgres.svc.cluster.local"
     :handler handler
     :log log))
