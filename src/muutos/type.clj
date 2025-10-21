(ns ^:no-doc muutos.type
  "Clojure representations of PostgreSQL data types.")

(defrecord LogSequenceNumber
  [segment byte-offset])

(defrecord Inet
  [address type netmask])

(defrecord Range
  [lower-bound
   lower-bound-inclusive?
   upper-bound
   upper-bound-inclusive?
   contain-empty?])

(defrecord Lexeme
  [text entries])

(defrecord Document
  [lexemes])

(defrecord Point
  [x y])

(defrecord LineSegment
  [point-1 point-2])

(defrecord Path
  [open? points])

(defrecord Box
  [upper-right lower-left])

(defrecord Polygon
  [points])

(defrecord Line
  [ax by c])

(defrecord Circle
  [x y r])
