(ns hive-datalevin.kg.store-lifecycle-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [datalevin.db]
            [datalevin.interface]
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

(deftest ensure-conn!-releases-a-stale-conn-before-reopening
  (let [s (mk-store (tmp-db-path))]
    (try
      (kg/transact! s [{:kg-edge/id "e1" :kg-edge/from "a" :kg-edge/to "b"
                        :kg-edge/relation :calls :kg-edge/scope "t"}])
      (let [stale      (ci/snapshot (:conn-init s))
            released   (atom [])
            real-close d/close]
        ;; Close the LMDB store under the conn, bypassing d/close, so the stale
        ;; conn keeps its registry, listener and read-cache state.
        (datalevin.interface/close (.-store ^datalevin.db.DB @stale))
        (with-redefs [d/close (fn [c] (swap! released conj c) (real-close c))]
          (testing "the read heals"
            (is (= 1 (kg/query s '[:find (count ?e) . :where [?e :kg-edge/scope "t"]])))))
        (testing "the stale conn went through datalevin's close before being dropped"
          (is (some #(identical? stale %) @released)))
        (testing "the cached conn is a fresh one"
          (is (not (identical? stale (ci/snapshot (:conn-init s)))))))
      (finally (kg/close! s)))))

(deftest genuine-errors-still-surface
  (let [s (mk-store (tmp-db-path))]
    (try
      (kg/ensure-conn! s)
      (testing "a non-dead-conn failure propagates — heal never launders real errors"
        (is (thrown? Exception
                     (kg/transact! s [[:db/add "no such entity spec" :nope]]))))
      (finally (kg/close! s)))))

;; ── global read-cache release ──────────────────────────────────────────────
;; datalevin.db keeps one LRUCache per store dir in a process-global map that
;; datalevin.core/close never clears; the store must release it on close.

(defn- read-caches ^java.util.Map [] @#'datalevin.db/caches)

(defn- conn-dir [s]
  (datalevin.interface/dir (.-store ^datalevin.db.DB @(ci/snapshot (:conn-init s)))))

(defn- seed! [s]
  (kg/transact! s [{:kg-edge/id "e1" :kg-edge/from "a" :kg-edge/to "b"
                    :kg-edge/relation :calls :kg-edge/scope "t"}])
  (kg/query s '[:find (count ?e) . :where [?e :kg-edge/scope "t"]]))

(deftest close!-releases-the-global-read-cache
  (let [s   (mk-store (tmp-db-path))
        _   (seed! s)
        dir (conn-dir s)]
    (is (.containsKey (read-caches) dir) "precondition: open store has a cache entry")
    (kg/close! s)
    (is (not (.containsKey (read-caches) dir)))))

(deftest delete-database!-releases-the-global-read-cache
  (let [s   (mk-store (tmp-db-path))
        _   (seed! s)
        dir (conn-dir s)]
    (kg/delete-database! s :i-mean-it)
    (is (not (.containsKey (read-caches) dir)))))

(deftest reset-conn!-releases-the-old-cache-and-stays-usable
  (let [s   (mk-store (tmp-db-path))
        _   (seed! s)
        dir (conn-dir s)
        old (.get (read-caches) dir)]
    (try
      (kg/reset-conn! s)
      (is (= 1 (kg/query s '[:find (count ?e) . :where [?e :kg-edge/scope "t"]])))
      (is (not (identical? old (.get (read-caches) dir)))
          "the pre-reset LRUCache is no longer referenced by the global map")
      (finally (kg/close! s)))))

(deftest close!-keeps-the-cache-of-a-store-still-shared-on-the-same-dir
  (let [path (tmp-db-path)
        s1   (mk-store path)
        s2   (mk-store path)]
    (try
      (seed! s1)
      (kg/ensure-conn! s2)
      (kg/close! s1)
      (testing "the surviving store on the same dir still reads"
        (is (= 1 (kg/query s2 '[:find (count ?e) . :where [?e :kg-edge/scope "t"]]))))
      (finally (kg/close! s1) (kg/close! s2)))))

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
