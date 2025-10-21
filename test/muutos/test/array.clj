(ns muutos.test.array
  (:import (java.util Arrays)))

(set! *warn-on-reflection* true)

(defprotocol Array
  (equals? [this other]))

(extend-protocol Array
  boolean/1 (equals? [this ^boolean/1 other] (Arrays/equals this other))
  byte/1 (equals? [this ^byte/1 other] (Arrays/equals this other))
  char/1 (equals? [this ^char/1 other] (Arrays/equals this other))
  long/1 (equals? [this ^long/1 other] (Arrays/equals this other))
  short/1 (equals? [this ^short/1 other] (Arrays/equals this other))
  int/1 (equals? [this ^int/1 other] (Arrays/equals this other))
  String/1 (equals? [this ^String/1 other] (Arrays/equals this other))
  float/1 (equals? [this ^float/1 other] (Arrays/equals this other))
  double/1 (equals? [this ^double/1 other] (Arrays/equals this other)))

(defmacro array?
  [x]
  `(some-> ~x class .isArray))
