(ns hive-datalevin.kg.recovery-shutdown-test
  "Never-NUKE-Data pinning for open failures that are not corruption.

   A stub opener (the store's `:opener` port, or the thunk handed to
   `heal-and-open!`) throws the failure under test; a recording strategy fn
   and a byte snapshot of a temp store dir observe what recovery did. The
   real store under ~/.local/share/hive-mcp is never opened: every fixture is
   a fresh temp dir.

   Mutants are self-contained: none calls the subject var, which the
   trifecta has rebound to the mutant by then."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.generators :as gen]
            [hive-test.trifecta :refer [deftrifecta]]
            [hive-spi.kg.protocol :as kg]
            [hive-datalevin.kg.recovery :as rec]
            [hive-datalevin.kg.store :as store]))
;; SPDX-License-Identifier: MIT

;; -----------------------------------------------------------------------------
;; Fixtures: temp store dir + byte snapshot
;; -----------------------------------------------------------------------------

(defn- temp-store-dir!
  "Fresh temp dir shaped like a datalevin store: data.mdb, lock.mdb and a
   txlog segment. Never under the real hive-mcp data dir."
  []
  (let [root (.toFile (java.nio.file.Files/createTempDirectory
                       "hiw1-dtlv-shutdown"
                       (make-array java.nio.file.attribute.FileAttribute 0)))
        db   (io/file root "kg")]
    (io/make-parents (io/file db "txlog" "seg"))
    (spit (io/file db "data.mdb") "healthy-store-bytes")
    (spit (io/file db "lock.mdb") "lock")
    (spit (io/file db "txlog" "segment-0001.wal") "wal-bytes")
    (.getAbsolutePath db)))

(defn- snapshot
  "Map of path (relative to the dir's parent) -> file bytes as a vector, for
   everything under the parent. Captures renames (quarantine) and siblings
   (heal backups) as well as content changes."
  [db-path]
  (let [parent (.getParentFile (io/file db-path))
        base   (.toPath parent)]
    (into (sorted-map)
          (for [^java.io.File f (file-seq parent)
                :when (.isFile f)]
            [(str (.relativize base (.toPath f)))
             (vec (java.nio.file.Files/readAllBytes (.toPath f)))]))))

(defn- delete-tree! [db-path]
  (doseq [^java.io.File f (reverse (file-seq (.getParentFile (io/file db-path))))]
    (.delete f)))

;; -----------------------------------------------------------------------------
;; Subject adapter
;; -----------------------------------------------------------------------------

(def ^:private failures
  "Name -> 0-arg exception factory for the stub opener."
  {:shutdown         #(IllegalStateException. "Shutdown in progress")
   :wrapped-shutdown #(RuntimeException. "open-kv failed"
                                         (IllegalStateException. "Shutdown in progress"))
   :unknown          #(Exception. "totally unrelated failure")})

(def ^:private ladders
  "Name -> strategy. :recording is resolved per run to a fn that records rungs."
  {:full-ladder [:truncate :quarantine :throw]
   :truncate    :truncate
   :quarantine  :quarantine
   :recording   :recording})

(defn run-open-scenario
  "Unary adapter: {:failure kw :ladder kw} -> observed outcome map.
   Runs heal-and-open! against a fresh temp store with a stub opener that
   always throws the named failure, 3 attempts allowed."
  [{:keys [failure ladder]}]
  (let [db-path (temp-store-dir!)
        before  (snapshot db-path)
        opens   (atom 0)
        rungs   (atom [])
        strat   (if (= :recording (ladders ladder))
                  (fn [c _ _] (swap! rungs conj c) :retry)
                  (ladders ladder))
        ex      (try (rec/heal-and-open!
                      {:policy {:strategy strat :max-attempts 3} :db-path db-path}
                      (fn [] (swap! opens inc) (throw ((failures failure)))))
                     nil
                     (catch clojure.lang.ExceptionInfo e e))
        after   (snapshot db-path)]
    (delete-tree! db-path)
    {:opens          @opens
     :rungs          (count @rungs)
     :classification (:classification (ex-data ex))
     :retryable?     (:retryable? (ex-data ex))
     :err            (:err (ex-data ex))
     :dir-intact?    (= before after)}))

(def ^:private safe-outcome?
  (fn [{:keys [opens rungs retryable? dir-intact?]}]
    (and (= 1 opens) (zero? rungs) (false? retryable?) dir-intact?)))

(deftrifecta non-corruption-open-failure-contract
  hive-datalevin.kg.recovery-shutdown-test/run-open-scenario
  {:golden-path "test/golden/kg/recovery-shutdown.edn"
   :cases       {:shutdown-full-ladder   {:failure :shutdown :ladder :full-ladder}
                 :shutdown-truncate      {:failure :shutdown :ladder :truncate}
                 :shutdown-quarantine    {:failure :shutdown :ladder :quarantine}
                 :shutdown-recording     {:failure :shutdown :ladder :recording}
                 :wrapped-shutdown       {:failure :wrapped-shutdown :ladder :full-ladder}
                 :unknown-full-ladder    {:failure :unknown :ladder :full-ladder}
                 :unknown-truncate       {:failure :unknown :ladder :truncate}
                 :unknown-recording      {:failure :unknown :ladder :recording}}
   :gen         (gen/let [f (gen/elements (keys failures))
                          l (gen/elements (keys ladders))]
                  {:failure f :ladder l})
   :pred        safe-outcome?
   :num-tests   40
   :mutations   [["retries-like-the-old-ladder"
                  (fn [_] {:opens 3 :rungs 2 :classification :unknown
                           :retryable? true :err :storage/heal-exhausted
                           :dir-intact? true})]
                 ["runs-one-rung-before-aborting"
                  (fn [{:keys [failure]}]
                    {:opens 1 :rungs 1
                     :classification (if (= :unknown failure) :unknown :not-corruption)
                     :retryable? false :err :storage/open-aborted :dir-intact? true})]
                 ["quarantines-the-store"
                  (fn [{:keys [failure]}]
                    {:opens 2 :rungs 0
                     :classification (if (= :unknown failure) :unknown :not-corruption)
                     :retryable? false :err :storage/open-aborted :dir-intact? false})]
                 ["shutdown-classified-unknown"
                  (fn [_] {:opens 1 :rungs 0 :classification :unknown
                           :retryable? false :err :storage/open-aborted
                           :dir-intact? true})]]
   :assert      (fn []
                  (let [out (run-open-scenario {:failure :shutdown :ladder :full-ladder})]
                    (is (= :not-corruption (:classification out))
                        "Shutdown in progress is not corruption")
                    (is (safe-outcome? out)
                        "one open, zero rungs, store bytes untouched"))
                  (is (zero? (:rungs (run-open-scenario {:failure :unknown :ladder :recording})))
                      ":unknown never reaches a recovery rung"))})

;; -----------------------------------------------------------------------------
;; Classification + healable gate
;; -----------------------------------------------------------------------------

(deftest classify-shutdown-test
  (is (= :not-corruption (rec/classify-open-failure ((failures :shutdown)))))
  (is (= :not-corruption (rec/classify-open-failure ((failures :wrapped-shutdown)))))
  (testing "shutdown wins even when a corrupt-looking message wraps it"
    (is (= :not-corruption
           (rec/classify-open-failure
            (Exception. "txn-log open" (IllegalStateException. "Shutdown in progress")))))))

(deftest only-positive-classes-are-healable-test
  (is (false? (rec/retryable-classification? :not-corruption)))
  (is (false? (rec/retryable-classification? :unknown)))
  (is (not (contains? rec/healable-classifications :unknown)))
  (is (contains? rec/healable-classifications :wal-corrupt)))

;; -----------------------------------------------------------------------------
;; ensure-conn! through the store's ports
;; -----------------------------------------------------------------------------

(deftest ensure-conn!-refuses-during-shutdown-test
  (let [db-path (temp-store-dir!)
        before  (snapshot db-path)
        opens   (atom 0)
        s       (store/create-store
                 {:db-path         db-path
                  :recovery-policy {:strategy [:truncate :quarantine :throw]
                                    :max-attempts 3}
                  :shutting-down?  (constantly true)
                  :opener          (fn [& _] (swap! opens inc) :conn)})
        ex      (try (kg/ensure-conn! s) nil
                     (catch clojure.lang.ExceptionInfo e e))]
    (try
      (is (= :storage/shutdown-in-progress (:err (ex-data ex))))
      (is (zero? @opens) "the opener is never called once shutdown has begun")
      (is (= before (snapshot db-path)) "store dir byte-untouched")
      (finally (delete-tree! db-path)))))

(deftest ensure-conn!-shutdown-opener-runs-no-rung-test
  (let [db-path (temp-store-dir!)
        before  (snapshot db-path)
        opens   (atom 0)
        s       (store/create-store
                 {:db-path         db-path
                  :recovery-policy {:strategy [:truncate :quarantine :throw]
                                    :max-attempts 3}
                  :shutting-down?  (constantly false)
                  :opener          (fn [& _]
                                     (swap! opens inc)
                                     (throw (IllegalStateException. "Shutdown in progress")))})
        ex      (try (kg/ensure-conn! s) nil
                     (catch Throwable e e))
        data    (some ex-data (take-while some? (iterate ex-cause ex)))]
    (try
      (is (= 1 @opens) "no retry")
      (is (= :not-corruption (:classification data)))
      (is (= before (snapshot db-path)) "store dir byte-untouched")
      (finally (delete-tree! db-path)))))

(deftest jvm-shutting-down?-false-in-a-live-jvm-test
  (is (false? (rec/jvm-shutting-down?))))
