(ns muutos.test.concurrency
  (:import (java.util.concurrent CountDownLatch Executors)))

(set! *warn-on-reflection* true)

(defmacro concurrently
  [{:keys [threads]
    :or {threads 1000}} & body]
  `(with-open [executor# (Executors/newVirtualThreadPerTaskExecutor)]
     (let [latch# (CountDownLatch. ~threads)]
       (dotimes [_i# ~threads]
         (.execute executor#
           (^:once fn* []
            (.countDown latch#)
            (.await latch#)
            (do ~@body)))))))
