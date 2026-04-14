(ns ^:no-doc muutos.impl.statement
  (:require [muutos.impl.client :as client]
            [muutos.impl.error :refer [handle!]]
            [muutos.impl.lockable :refer [with-lock]]
            [muutos.codec.bin :as bin]))

(defn close [client stmt-name]
  (let [stmt-name (name stmt-name)]
    (with-lock client
      (client/enqueue client {:type :close :target :statement :name stmt-name})
      (client/enqueue client {:type :sync})
      (client/flush client)

      (loop [ex nil]
        (let [{:keys [type] :as response} (client/recv client)]
          (case type
            :ready-for-query (when ex (throw ex))
            :error (recur (response :ex))
            :close-complete (recur ex)))))))

(def ^:private unnamed-portal "")

(defn execute [client stmt-name parameters]
  (let [encoded-parameters (mapv bin/encode parameters)]
    (client/enqueue client {:type :bind :statement stmt-name :portal unnamed-portal :parameters encoded-parameters})
    (client/enqueue client {:type :execute :portal unnamed-portal :max-rows 0})
    (client/enqueue client {:type :sync})
    (client/flush client)))

(defn parse [client stmt-name q oids]
  (client/enqueue client {:type :parse :statement stmt-name :query q :oids oids})
  (client/enqueue client {:type :describe :target :statement :name stmt-name})
  (client/enqueue client {:type :sync})
  (client/flush client)

  (loop [data []
         attrs {}
         ex nil]
    (let [{:keys [type] :as response} (client/recv client)]
      (case type
        :ready-for-query
        (if ex
          (handle! client ex)
          attrs)

        :row-description
        (recur data (response :attrs) ex)

        (:parameter-description :no-data :parse-complete)
        (recur data attrs ex)

        :error
        (recur data attrs (response :ex))))))
