(ns ^:no-doc muutos.impl.anomaly
  (:require [cognitect.anomalies :as-alias anomalies]))

(defmacro anomaly!
  "Given an exception message, an anomaly category, and optional ex-data, and
  optional cause, throw an exception indicating the given anomaly."
  ([msg category]
   `(anomaly! ~msg ~category nil))
  ([msg category data]
   `(anomaly! ~msg ~category ~data nil))
  ([msg category data cause]
   `(throw (ex-info ~msg (merge ~data {::anomalies/category ~category}) ~cause))))

(comment
  (anomaly! "Boom!" ::anomalies/fault)
  (anomaly! "Boom!" ::anomalies/fault {:extra :data})
  ,,,)
