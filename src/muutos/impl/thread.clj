(ns ^:no-doc muutos.impl.thread
  "Utilities for working with threads."
  (:import (java.util.concurrent BlockingQueue Executors ExecutorService LinkedBlockingQueue ThreadFactory TimeUnit)
           (java.util.concurrent RejectedExecutionException RejectedExecutionHandler ThreadPoolExecutor ThreadPoolExecutor$AbortPolicy)
           (java.time Duration)))

(set! *warn-on-reflection* true)

(defmacro execute
  "Execute a body in the given java.util.concurrent.Executor."
  [executor & body]
  `(ExecutorService/.execute ~executor (^:once fn* [] (do ~@body))))

(defn factory
  "Given a thread name, return a ThreadFactory that gives every thread the
  given name."
  [^String thread-name]
  (reify ThreadFactory
    (newThread [_ runnable]
      (Thread. ^Runnable runnable thread-name))))

(defn flow-control-policy
  "Given an executor work queue (a java.util.concurrent.BlockingQueue) and
  options, return a handler for rejected tasks that waits for available
  capacity in the executor.

  Options:

    :timeout (java.time.Duration)
       The duration to wait for available capacity.

       By default, wait (practically) indefinitely."
  [queue & {:keys [timeout] :or {timeout (Duration/ofNanos Long/MAX_VALUE)}}]
  (let [nanos (Duration/.toNanos timeout)]
    (reify RejectedExecutionHandler
      (rejectedExecution [_ runnable _executor]
        (try
          (when-not (BlockingQueue/.offer queue runnable nanos TimeUnit/NANOSECONDS)
            (throw (RejectedExecutionException. "Timed out due to backpressure")))
          (catch InterruptedException ex
            (.interrupt (Thread/currentThread))
            (throw (RejectedExecutionException. ex))))))))

(defn executor
  "Given an options map, return a new (unconfigurable) executor service.

  For an explanation of all the options, see the Javadoc for java.util.concurrent.ThreadPoolExecutor.

  For default options, see the values in the :or deconstructor."
  ^ExecutorService
  [& {:keys [core-pool-size max-pool-size keep-alive-time time-unit work-queue rejection-policy thread-factory]
      :or {core-pool-size 1
           max-pool-size 1
           keep-alive-time 0
           time-unit TimeUnit/MILLISECONDS
           work-queue (LinkedBlockingQueue.)
           rejection-policy (ThreadPoolExecutor$AbortPolicy.)
           thread-factory (Executors/defaultThreadFactory)}}]
  (Executors/unconfigurableExecutorService
    (ThreadPoolExecutor. ^int core-pool-size ^int max-pool-size ^long keep-alive-time time-unit
      work-queue
      thread-factory
      rejection-policy)))
