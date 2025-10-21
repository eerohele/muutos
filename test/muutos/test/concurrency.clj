(ns muutos.test.concurrency
  (:import (java.util.concurrent CountDownLatch Executors)
           (java.util.concurrent TimeUnit)))

(set! *warn-on-reflection* true)

(defmacro concurrently
  [{:keys [threads]
    :or {threads 1000}} & body]
  `(let [executor# (Executors/newVirtualThreadPerTaskExecutor)]
     (try
       (let [latch# (CountDownLatch. ~threads)]
         (dotimes [_i# ~threads]
           (.execute executor#
             (^:once fn* []
              (.countDown latch#)
              (.await latch#)
              (do ~@body)))))
       (finally
         (.shutdown executor#)
         (.awaitTermination executor#
           Long/MAX_VALUE TimeUnit/MILLISECONDS)))))
