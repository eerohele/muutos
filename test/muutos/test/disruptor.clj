(ns muutos.test.disruptor
  (:refer-clojure :exclude [proxy])
  (:import (java.lang AutoCloseable)
           (org.netcrusher.core.reactor NioReactor)
           (org.netcrusher.tcp TcpCrusherBuilder)))

(set! *warn-on-reflection* true)

(defn proxy [& {:keys [bind-address bind-port connect-address connect-port]
                :or {bind-address "localhost" connect-address "localhost"}}]
  (let [reactor (NioReactor.)
        crusher (->
                  (TcpCrusherBuilder/builder)
                  (TcpCrusherBuilder/.withReactor reactor)
                  (TcpCrusherBuilder/.withBindAddress bind-address bind-port)
                  (TcpCrusherBuilder/.withConnectAddress connect-address connect-port)
                  (TcpCrusherBuilder/.buildAndOpen))]
    (reify AutoCloseable
      (close [_]
        (AutoCloseable/.close crusher)
        (AutoCloseable/.close reactor)))))
