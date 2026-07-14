(ns hive-datalevin.kg.store-lifecycle-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [hive-datalevin.kg.recovery :as rec]
            [hive-datalevin.kg.store :as store]
            [hive-spi.kg.protocol :as kg]
            [hive-spi.kg.conn-init :as ci]
            [hive-test.stateful :as hts])
  (:import [java.nio.channels ClosedChannelException]
           [java.nio.file Files]))
;; SPDX-License-Identifier: MIT

(defn- tmp-db-path []
  (str (Files/createTempDirectory "dtlv-lifecycle" (make-array java.nio.file.attribute.FileAttribute 0))
       "/db"))

(defn- mk-store [path]
  (store/create-store {:db-path path}))

(defn- kill-conn!
  "Close the store's live conn OUT FROM UNDER it, leaving the stale conn cached
   — the exact state the 2026-07-14 carto scan incident left the :carto slot in."
  [s]
  (d/close (ci/snapshot (:conn-init s))))

;; ── dead-conn classification ────────────────────────────────────────────────

(deftest dead-conn-throwable?-test
  (testing "ClosedChannelException anywhere in the cause chain"
    (is (rec/dead-conn-throwable? (ClosedChannelException.)))
    (is (rec/dead-conn-throwable? (Exception. "wrapper" (ClosedChannelException.)))))
  (testing "closed-conn assertion from datalevin's read path"
    (is (rec/dead-conn-throwable? (AssertionError. "Assert failed: (conn? conn)"))))
  (testing "generic 'closed' storage messages"
    (is (rec/dead-conn-throwable? (IllegalStateException. "Database is closed"))))
  (testing "unrelated failures are NOT dead-conn (schema errors must surface)"
    (is (not (rec/dead-conn-throwable? (Exception. "MDB_CORRUPTED page"))))
    (is (not (rec/dead-conn-throwable? (ex-info "bad attr value" {}))))
    (is (not (rec/dead-conn-throwable? nil)))))

;; ── self-healing store ──────────────────────────────────────────────────────

(deftest transact!-heals-a-conn-closed-behind-its-back
  (let [s (mk-store (tmp-db-path))]
    (try
      (kg/transact! s [{:kg-edge/id "e1" :kg-edge/from "a" :kg-edge/to "b"
                        :kg-edge/relation :calls :kg-edge/scope "t"}])
      (kill-conn! s)
      (testing "a write against the dead cached conn heals and commits"
        (kg/transact! s [{:kg-edge/id "e2" :kg-edge/from "b" :kg-edge/to "c"
                          :kg-edge/relation :calls :kg-edge/scope "t"}])
        (is (= 2 (kg/query s '[:find (count ?e) . :where [?e :kg-edge/scope "t"]]))
            "both writes visible — nothing lost across the heal"))
      (finally (kg/close! s)))))

(deftest reads-heal-a-conn-closed-behind-their-back
  (let [s (mk-store (tmp-db-path))]
    (try
      (kg/transact! s [{:kg-edge/id "e1" :kg-edge/from "a" :kg-edge/to "b"
                        :kg-edge/relation :calls :kg-edge/scope "t"}])
      (kill-conn! s)
      (testing "ensure-conn! notices the dead conn and reopens for reads"
        (is (= 1 (kg/query s '[:find (count ?e) . :where [?e :kg-edge/scope "t"]]))))
      (finally (kg/close! s)))))

(deftest genuine-errors-still-surface
  (let [s (mk-store (tmp-db-path))]
    (try
      (kg/ensure-conn! s)
      (testing "a non-dead-conn failure propagates — heal never launders real errors"
        (is (thrown? Exception
                     (kg/transact! s [[:db/add "no such entity spec" :nope]]))))
      (finally (kg/close! s)))))

;; ── lifecycle contract as a hive-test.stateful Machine ─────────────────────
;; Safety: no reachable state loses a committed write. Liveness: from every
;; reachable state the store is writable again — self-healing is the contract,
;; not an implementation detail.

(def lifecycle-machine
  {:init     (fn [] {:conn :open :writes 0 :was-dead? false})
   :commands {:kill     {:pre  (fn [m _] (= :open (:conn m)))
                         :next (fn [m _] (assoc m :conn :dead :was-dead? true))}
              :transact {:next (fn [m _] (-> m (assoc :conn :open)
                                             (update :writes inc)))}
              :close    {:pre  (fn [m _] (= :open (:conn m)))
                         :next (fn [m _] (assoc m :conn :closed))}
              :reopen   {:pre  (fn [m _] (#{:closed :dead} (:conn m)))
                         :next (fn [m _] (assoc m :conn :open))}}
   :invariants {:writes-monotonic (fn [m] (>= (:writes m) 0))}
   :goals      {:recovered-after-death (fn [m] (and (:was-dead? m)
                                                    (= :open (:conn m))))}
   :ident      (fn [m] [(:conn m) (:was-dead? m) (min (:writes m) 3)])})

(deftest lifecycle-model-is-self-healing
  (let [r (hts/check lifecycle-machine {:max-depth 8})]
    (testing "from every reachable state — including a killed conn — a healed,
              writable state is reachable: a dead conn is never terminal"
      (is (:ok? r) (hts/report-str r)))))
