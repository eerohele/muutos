(ns user
  (:require [time-literals.read-write :as time-literals]))

(time-literals/print-time-literals-clj!)

(defn check! [& _args]
  (run! (requiring-resolve 'cognitect.transcriptor/run)
    ((requiring-resolve 'cognitect.transcriptor/repl-files) "xr")))

(defn pgbench! [& _args]
  ((requiring-resolve 'cognitect.transcriptor/run) "dev/src/pgbench.repl"))

(comment
  (check!)
  ,,,)
