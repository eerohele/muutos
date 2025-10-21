(ns muutos.dev.proxy
  "A TCP proxy for testing."
  (:require [muutos.impl.thread :as thread])
  (:import (java.io InputStream OutputStream)
           (java.net InetAddress ServerSocket Socket)
           (java.util.concurrent Executors)))

(set! *warn-on-reflection* true)

(defmacro ^:private until-interruption
  "Given a body, execute the body until the current thread is interrupted."
  [& body]
  `(while (not (.isInterrupted (Thread/currentThread)))
     (do ~@body)))

(defn run
  [upstream-host upstream-port
   & {:keys [port] :or {port 5433}}]
   (let [acceptor (Executors/newVirtualThreadPerTaskExecutor)
         worker (Executors/newFixedThreadPool 2)
         server-socket (ServerSocket. port 0 (InetAddress/getLoopbackAddress))
         upstream-socket (Socket. ^String upstream-host ^long upstream-port)]
     (thread/execute acceptor
       (until-interruption
         (let [client-socket (.accept server-socket)]
           (thread/execute worker
             (with-open [^OutputStream proxy->client (.getOutputStream client-socket)
                         upstream->proxy (.getInputStream upstream-socket)]
               (try
                 (.transferTo upstream->proxy proxy->client)
                 (finally
                   (.close client-socket)
                   (.close upstream-socket)))))

           (thread/execute worker
             (with-open [proxy->upstream (.getOutputStream upstream-socket)
                         ^InputStream client->proxy (.getInputStream client-socket)]
               (.transferTo client->proxy proxy->upstream))))))
     (fn []
       (.close upstream-socket)
       (.close server-socket)
       (.shutdownNow worker)
       (.shutdownNow acceptor)
       :stopped)))

(comment
  (def proxy (run "localhost" 5432))
  (proxy)
  ,,,)
