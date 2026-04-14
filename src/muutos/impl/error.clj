(ns ^:no-doc muutos.impl.error
  (:require [cognitect.anomalies :as-alias anomalies]
            [muutos.error :as-alias error]
            [muutos.impl.anomaly :refer [anomaly!]])
  (:import (java.lang AutoCloseable)))

(set! *warn-on-reflection* true)

(defn handle! [client ex]
  (if (= ::error/server-error (-> ex ex-data :kind))
    (throw ex)
    (do
      (AutoCloseable/.close client)
      (anomaly! "Fatal error when reading server response; closing client to prevent protocol desynchronization" ::anomalies/fault (ex-data ex) ex))))
