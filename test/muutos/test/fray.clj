(ns muutos.test.fray
  (:import (kotlin.jvm.functions Function1)
           (org.pastalab.fray.junit.plain FrayInTestLauncher)
           (org.pastalab.fray.core.scheduler POSScheduler)
           (org.pastalab.fray.core.randomness ControlledRandom)))

(set! *warn-on-reflection* true)

(defmacro run
  [{:keys [iterations timeout replay?]
    :or {iterations 1000 timeout 120 replay? false}}
   & body]
  `(.launchFray FrayInTestLauncher/INSTANCE
     (^:once fn* [] (do ~@body))
     (POSScheduler.)
     (ControlledRandom.)
     ~iterations
     ~timeout
     ~replay?
     (reify Function1 (invoke [this _]))))
