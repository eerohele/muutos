(ns muutos.test.tcp-crusher
  (:import (java.lang AutoCloseable)
           (org.netcrusher.core.reactor NioReactor)
           (org.netcrusher.tcp TcpCrusherBuilder)))

(set! *warn-on-reflection* true)

(defn make-reactor ^AutoCloseable []
  (NioReactor.))

(defn make-crusher ^AutoCloseable [reactor & {:keys [bind-address bind-port connect-address connect-port]
                                              :or {bind-address "localhost" connect-address "localhost"}}]
  (->
    (TcpCrusherBuilder/builder)
    (TcpCrusherBuilder/.withReactor reactor)
    (TcpCrusherBuilder/.withBindAddress bind-address bind-port)
    (TcpCrusherBuilder/.withConnectAddress connect-address connect-port)
    (TcpCrusherBuilder/.buildAndOpen)))
