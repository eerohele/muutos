(ns ^:no-doc muutos.impl.lockable)

(set! *warn-on-reflection* true)

(defprotocol Lockable
  (lock ^java.util.concurrent.locks.Lock [this]))

(defmacro with-lock
  "Given a java.util.concurrent.locks.Lock and a body, execute the body while
  holding the given lock."
  [lockable & body]
  `(do
     (.lock (lock ~lockable))
     (try
       (do ~@body)
       (finally
         (.unlock (lock ~lockable))))))
