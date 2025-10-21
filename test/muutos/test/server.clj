(ns muutos.test.server
  (:require [clj-test-containers.core :as tc]
            [muutos.impl.hook :as hook])
  (:import (java.lang AutoCloseable)))

(defprotocol Server
  (host [this])
  (port [this]))

(defn start [opts]
  (let [server (let [container (tc/start! (tc/create opts))]
                 (reify
                   Server
                   (host [_] (:host container))
                   (port [_] (get (:mapped-ports container) 5432))
                   AutoCloseable
                   (close [_] (tc/stop! container))))]
    (hook/on-shutdown (.close server))
    server))
