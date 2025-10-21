(ns muutos.impl.decode-test
  (:require [clojure.spec.alpha :as spec]
            [clojure.spec.gen.alpha :as gen]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.properties :as prop]
            [muutos.dev.encode :as encode]
            [muutos.impl.decode :as decode]
            [muutos.specs.logical-replication-message :as lrm]))

(defmulti comparable :type)

(defmethod comparable :default [message] message)

(defmethod comparable :message [message]
  ;; Can't use = to compare byte arrays, so must convert them to vectors first.
  (update message :content vec))

(defn ^:private round-trip [message options]
  (decode/decode-logical-replication-message (encode/encode message) options))

(defn ^:private has-optional-xid? [message]
  (#{:message :relation :type :insert :update :delete :truncate} (:type message)))

(defn ^:private stream-abort-parallel? [state message]
  (and (= :stream-abort (:type message))
    (= 4 (:protocol-version state)) (= :parallel (:streaming state))))

(defspec round-trip-logical-replication-message 20000
  (prop/for-all [protocol-version (gen/elements #{2 3 4})
                 streaming (gen/elements #{true false :parallel})
                 tx-state (gen/elements #{:complete :in-progress})
                 xid (spec/gen ::lrm/xid)
                 abort-lsn (spec/gen ::lrm/abort-lsn)
                 tx-timestamp (spec/gen ::lrm/tx-timestamp)
                 message (spec/gen ::lrm/message)]
    (let [state {:protocol-version protocol-version
                 :streaming streaming
                 :tx-state tx-state}
          message (cond-> message
                    (and (has-optional-xid? message) (= :complete tx-state))
                    (dissoc :xid)
                    (and (has-optional-xid? message) (= :in-progress tx-state))
                    (assoc :xid xid)
                    (stream-abort-parallel? state message)
                    (assoc :abort-lsn abort-lsn)
                    (stream-abort-parallel? state message)
                    (assoc :tx-timestamp tx-timestamp)
                    (not (stream-abort-parallel? state message))
                    (dissoc :abort-lsn :tx-timestamp))]
      (= (comparable message)
        (comparable (round-trip message state))))))

(comment
  (round-trip-logical-replication-message)
  (def message (-> *1 :shrink :smallest peek))
  ,,,)
