(ns ^:no-doc muutos.impl.time
  "Utilities for working with PostgreSQL times (e.g. the Postgres epoch)."
  (:import (java.time Duration LocalDate LocalDateTime LocalTime Instant ZoneOffset)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def ^Instant postgres-epoch
  "A java.time.Instant marking the Postgres epoch."
  (Instant/parse "2000-01-01T00:00:00.000000Z"))

(def ^LocalDate postgres-epoch-date
  (LocalDate/ofInstant postgres-epoch ZoneOffset/UTC))

(def ^LocalTime postgres-epoch-time
  (LocalTime/parse "00:00:00.000000"))

(def ^LocalDateTime postgres-epoch-date-time
  (LocalDateTime/ofInstant postgres-epoch ZoneOffset/UTC))

(defn microseconds-since-postgres-epoch
  "Return the number (long) of microseconds since the Postgres epoch."
  []
  (let [now (Instant/now)
        duration (Duration/between postgres-epoch now)]
    (quot (.toNanos duration) 1000)))

(comment (microseconds-since-postgres-epoch) ,,,)
