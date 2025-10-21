(ns ^:no-doc muutos.impl.specs.gen
  "clojure.test.check generators for PostgreSQL logical replication messages
  represented as Clojure data."
  (:refer-clojure :exclude [double int float short])
  (:require [clojure.core :as core]
            [clojure.spec.gen.alpha :as gen]
            [muutos.impl.time :as time])
  (:import (java.time Duration Instant LocalTime LocalDate LocalDateTime Instant OffsetTime Period ZoneOffset)
           (java.time.temporal ChronoUnit)))

(set! *warn-on-reflection* true)

(def int
  (gen/fmap core/int (gen/int)))

(def short
  (gen/fmap core/short (gen/choose Short/MIN_VALUE Short/MAX_VALUE)))

(def float
  (gen/fmap core/float (gen/double* {:NaN? false :min Float/MIN_VALUE :max Float/MAX_VALUE})))

(def double
  (gen/double* {:NaN? false}))

(def ^:private ^Instant min-instant
  "The minimum Instant that's representable as a long of microseconds since the
  Postgres epoch."
  (.minus time/postgres-epoch Long/MAX_VALUE ChronoUnit/MICROS))

(def ^:private ^Instant max-instant
  "The maximum Instant that's representable as a long of microseconds since the
  Postgres epoch."
  (.plus time/postgres-epoch Long/MAX_VALUE ChronoUnit/MICROS))

(def ^:private gen-instant-millis
  (gen/choose (.toEpochMilli min-instant) (.toEpochMilli max-instant)))

(def instant
  (gen/fmap (fn [millis] (Instant/ofEpochMilli millis)) gen-instant-millis))

(def zone-offset
  (gen/fmap ZoneOffset/ofTotalSeconds
    (gen/choose (.getTotalSeconds ZoneOffset/MIN) (.getTotalSeconds ZoneOffset/MAX))))

(def local-time
  (gen/fmap LocalTime/ofSecondOfDay (gen/choose 0 (dec (.toSeconds (Duration/ofDays 1))))))

(def offset-time
  (gen/fmap
    (fn [[local-time offset]]
      (OffsetTime/of local-time offset))
    (gen/tuple local-time zone-offset)))

(def local-date
  (gen/fmap LocalDate/ofEpochDay (gen/choose 0 (.toEpochDay (LocalDate/now)))))

(def local-date-time
  (gen/fmap (fn [[date time]]
              (LocalDateTime/of date time)) (gen/tuple local-date local-time)))

(def duration
  (gen/fmap Duration/ofSeconds
    (gen/choose -8640000 8640000)))

(def period
  (gen/fmap Period/ofDays
    (gen/such-that (complement zero?)
      (gen/choose (- (* 365 10)) (* 365 10)))))

(def duration-or-period
  (gen/one-of [duration period]))
