(ns ^:no-doc muutos.impl.hook)

(set! *warn-on-reflection* true)

(defmacro on-shutdown
  "Execute body on runtime shutdown."
  [& body]
  `(.addShutdownHook (Runtime/getRuntime)
     (Thread. (^:once fn* [] (do ~@body)))))
