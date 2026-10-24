(ns ^:no-doc muutos.impl.client)

(set! *warn-on-reflection* true)

(defprotocol Client
  (id [this])
  (options [this])
  (log [this level event-name data])
  (connection [this])
  (oid [this x])
  (aux [this f])
  (stats [this]))
