(ns muutos.test.container
  (:require [clj-test-containers.core :as tc]
            [muutos.impl.hook :as hook]
            [muutos.test.server :refer [Server]])
  (:import (java.lang AutoCloseable)))

(def default-opts
  {:env-vars {"POSTGRES_PASSWORD" "postgres"
              "POSTGRES_DB" "test"}
   :exposed-ports [5432]
   :image-name "postgres:17"
   :wait-for {:strategy :log :message "accept connections"}})

(def create
  (memoize (fn [opts] (tc/create opts))))

(defn start
  [container]
  (let [server (let [container (tc/start! container)]
                 (reify
                   Server
                   (host [_] (:host container))
                   (port [_] (get (:mapped-ports container) 5432))
                   AutoCloseable
                   (close [_] (tc/stop! container))))]
    (hook/on-shutdown (.close server))
    server))
