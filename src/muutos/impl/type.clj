(ns ^:no-doc muutos.impl.type
  (:import (java.net InetAddress)
           (java.time LocalDate LocalTime LocalDateTime Instant OffsetTime Duration Period)
           (java.math BigDecimal)
           (java.util UUID)
           (muutos.type Point LineSegment Path Box Polygon Line Circle Inet LogSequenceNumber Range)))

(defn oid
  "Given an object, return its corresponding Postgres data type OID if any, else
  0."
  [x]
  (condp instance? x
    Boolean 16
    byte/1 17
    Character 18
    Short 21
    Integer 23
    Long 20
    String 25
    Point 600
    LineSegment 601
    Path 602
    Box 603
    Polygon 604
    Line 628
    Float 700
    Double 701
    Circle 718
    InetAddress 869
    boolean/1 1000
    char/1 1002
    long/1 1016
    short/1 1005
    int/1 1007
    String/1 1009
    float/1 1021
    double/1 1022
    BigInteger 790
    Inet 869
    LocalDate 1082
    LocalTime 1083
    LocalDateTime 1114
    OffsetTime 1266
    Instant 1184
    Duration 1186
    Period 1186
    BigDecimal 1700
    UUID 2950
    LogSequenceNumber 3220
    Range (condp instance? (or (:lower-bound x) (:upper-bound x))
            Integer 3904
            BigDecimal 3906
            LocalDateTime 3908
            Instant 3910
            LocalDate 3912
            Long 3926
            0)
    0))

(comment
  (oid true)
  (oid {})
  ,,,)
