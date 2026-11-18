(ns muutos.test.tcp-crusher
  (:import (java.io Closeable)
           (org.netcrusher.core.reactor NioReactor)
           (org.netcrusher.tcp TcpCrusherBuilder)))

(set! *warn-on-reflection* true)

(defn make-reactor ^Closeable []
  (NioReactor.))

(defn make-crusher ^Closeable [reactor & {:keys [bind-address bind-port connect-address connect-port]
                                          :or {bind-address "localhost" connect-address "localhost"}}]
  (->
    (TcpCrusherBuilder/builder)
    (TcpCrusherBuilder/.withReactor reactor)
    (TcpCrusherBuilder/.withBindAddress bind-address bind-port)
    (TcpCrusherBuilder/.withConnectAddress connect-address connect-port)
    (TcpCrusherBuilder/.buildAndOpen)))
