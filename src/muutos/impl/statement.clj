(ns ^:no-doc muutos.impl.statement
  (:require [muutos.impl.connection :as connection]
            [muutos.impl.error :refer [handle!]]
            [muutos.impl.lockable :refer [with-lock]]
            [muutos.codec.bin :as bin]))

(defn close [connection stmt-name]
  (let [stmt-name (name stmt-name)]
    (with-lock connection
      (connection/enqueue connection {:type :close :target :statement :name stmt-name})
      (connection/enqueue connection {:type :sync})
      (connection/flush connection)

      (loop [ex nil]
        (let [{:keys [type] :as response} (connection/read connection {})]
          (case type
            :ready-for-query (when ex (throw ex))
            :error (recur (response :ex))
            :close-complete (recur ex)))))))

(defn execute [connection stmt-name parameters & {:keys [portal max-rows] :or {portal "" max-rows 0}}]
  (let [encoded-parameters (mapv bin/encode parameters)]
    (connection/enqueue connection {:type :bind :statement stmt-name :portal portal :parameters encoded-parameters})
    (connection/enqueue connection {:type :execute :portal portal :max-rows max-rows})
    (connection/enqueue connection {:type :sync})
    (connection/flush connection)))

(defn parse [client connection stmt-name q oids]
  (connection/enqueue connection {:type :parse :statement stmt-name :query q :oids oids})
  (connection/enqueue connection {:type :describe :target :statement :name stmt-name})
  (connection/enqueue connection {:type :sync})
  (connection/flush connection)

  (loop [data []
         attrs {}
         ex nil]
    (let [{:keys [type] :as response} (connection/read connection {})]
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
