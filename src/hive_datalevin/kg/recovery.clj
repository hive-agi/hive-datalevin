(ns hive-datalevin.kg.recovery
  "Datalevin store-open self-heal. Classify an LMDB open failure and apply a
   recovery policy so the store returns to health without operator action.
   `heal-and-open!` composes classification + policy + the caller's `open-fn`;
   it never opens the db itself, keeping this ns driver-agnostic at compile time."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [hive-dsl.result :refer [rescue]]
            [taoensso.timbre :as log]))
;; SPDX-License-Identifier: AGPL-3.0-or-later

;; -----------------------------------------------------------------------------
;; Classification — pure
;; -----------------------------------------------------------------------------

(def ^:private corrupt-signatures
  "Substrings indicating on-disk corruption. Conservative — ambiguous cases stay
   :unknown. Covers LMDB page errors (MDB_*) and datalevin txn-log corruption
   (upstream wording varies: txn-log / txlog / WAL)."
  ["MDB_CORRUPTED"
   "MDB_PAGE_NOTFOUND"
   "MDB_INVALID"
   "MDB_CURSOR_FULL"
   "Page not found"
   "WAL corrupt"
   "txlog corrupt"
   "Invalid txlog"
   "Invalid txn-log"
   "Txn-log segment"
   "txn-log corrupt"
   "txn-log"
   "checksum mismatch"])

(def ^:private lock-signatures
  ["Resource temporarily unavailable"
   "MDB_BUSY"
   "EAGAIN"
   "lock"])

(def ^:private version-signatures
  ["MDB_VERSION_MISMATCH"
   "version mismatch"
   "schema version"])

(defn- match-any?
  [^String msg signatures]
  (when msg
    (boolean (some #(str/includes? msg %) signatures))))

(defn classify-open-failure
  "Classify a Throwable raised by an LMDB-level open. Returns one of
   `:wal-corrupt`, `:lock-contention`, `:version-mismatch`, `:unknown`.
   Walks the cause chain so a wrapping exception still classifies correctly."
  [^Throwable ex]
  (loop [t ex]
    (let [msg (some-> t .getMessage str)]
      (cond
        (match-any? msg corrupt-signatures)    :wal-corrupt
        (match-any? msg version-signatures)    :version-mismatch
        (match-any? msg lock-signatures)       :lock-contention
        (and t (.getCause t))                  (recur (.getCause t))
        :else                                  :unknown))))

;; -----------------------------------------------------------------------------
;; Quarantine — IO
;; -----------------------------------------------------------------------------

(defn quarantine-path
  "Derive the quarantine path for `db-path`. Deterministic given a timestamp;
   defaults to `System/currentTimeMillis` for live calls."
  ([db-path] (quarantine-path db-path (System/currentTimeMillis)))
  ([db-path ts]
   (str db-path ".corrupt." ts)))

(defn quarantine!
  "Move the db directory aside to `quarantine-path`. Returns the new path on
   success, nil if the source doesn't exist. Throws on IO failure."
  ([db-path] (quarantine! db-path (quarantine-path db-path)))
  ([db-path target-path]
   (let [src (io/file db-path)]
     (when (.exists src)
       (let [dst (io/file target-path)]
         (when-not (.renameTo src dst)
           (throw (ex-info "Failed to quarantine corrupt datalevin store"
                           {:src db-path :dst target-path
                            :err :storage/quarantine-failed})))
         (log/warn "[storage/recovery] Quarantined corrupt datalevin store"
                   {:src db-path :dst target-path})
         target-path)))))

;; -----------------------------------------------------------------------------
;; Truncate — IO (datalevin txn-log tail-zero recovery)
;; -----------------------------------------------------------------------------

(defn- txlog-segment-dir
  "Locate the txn-log segment directory: `<db-path>/txlog/`."
  [db-path]
  (io/file db-path "txlog"))

(defn- segment-needs-heal?
  "Return :tail-zeroed if the segment loads only with
   `:allow-preallocated-tail? true`; :ok if strict scan succeeds;
   :unhealable if even lenient scan refuses the file."
  [^java.io.File seg-file]
  (let [path        (.getAbsolutePath seg-file)
        scan        (requiring-resolve 'datalevin.txlog/scan-segment)
        strict      (try (scan path {:collect-records? false
                                     :allow-preallocated-tail? false})
                         :ok
                         (catch Throwable _ :strict-failed))]
    (if (= :ok strict)
      :ok
      (try
        (let [lenient (scan path {:collect-records? false
                                  :allow-preallocated-tail? true})]
          (if (:partial-tail? lenient)
            :tail-zeroed
            :unhealable))
        (catch Throwable _ :unhealable)))))

(defn- forensic-copy!
  "Copy a segment file to a heal-backup sibling directory before truncate.
   Returns the backup path string."
  [^java.io.File seg-file db-path ts]
  (let [backup-dir (io/file (str db-path ".txlog-heal." ts))
        backup-fn  (io/file backup-dir
                            (str (.getName seg-file) ".before-truncate"))]
    (.mkdirs backup-dir)
    (io/copy seg-file backup-fn)
    (.getAbsolutePath backup-fn)))

(defn truncate-tail!
  "Walk the txn-log segment directory under `db-path`. For each segment whose
   strict scan fails but lenient scan reports `:partial-tail? true`, copy it
   aside and invoke datalevin's `truncate-partial-tail!`. Returns a vec of
   per-segment heal records; empty means no segment needed truncation."
  ([db-path] (truncate-tail! db-path (System/currentTimeMillis)))
  ([db-path ts]
   (let [seg-dir   (txlog-segment-dir db-path)
         seg-files (requiring-resolve 'datalevin.txlog/segment-files)
         truncate  (requiring-resolve 'datalevin.txlog/truncate-partial-tail!)
         segs      (when (.isDirectory seg-dir)
                     (seg-files (.getAbsolutePath seg-dir)))]
     (reduce
      (fn [acc {:keys [^java.io.File file]}]
        (try
          (case (segment-needs-heal? file)
            :ok          acc
            :tail-zeroed (let [backup (forensic-copy! file db-path ts)
                               result (truncate
                                       (.getAbsolutePath file)
                                       {:allow-preallocated-tail? true
                                        :collect-records? false})]
                           (log/warn "[storage/recovery] Truncated txn-log segment"
                                     {:path (.getAbsolutePath file)
                                      :backup backup
                                      :old-size (:old-size result)
                                      :new-size (:new-size result)
                                      :dropped-bytes (:dropped-bytes result)})
                           (conj acc (-> result
                                         (assoc :path (.getAbsolutePath file)
                                                :backup backup
                                                :sub-classification :tail-zeroed))))
            :unhealable  (do (log/warn "[storage/recovery] Segment unhealable — mid-segment corruption"
                                       {:path (.getAbsolutePath file)})
                             (conj acc {:path (.getAbsolutePath file)
                                        :truncated? false
                                        :sub-classification :mid-segment})))
          (catch Throwable t
            (log/error t "[storage/recovery] Segment heal failed"
                       {:path (.getAbsolutePath file)})
            (conj acc {:path (.getAbsolutePath file)
                       :truncated? false
                       :error (.getMessage t)}))))
      []
      segs))))

;; -----------------------------------------------------------------------------
;; Telemetry
;; -----------------------------------------------------------------------------

(defn- emit!
  "Best-effort dispatch into the host event bus. Late-resolves + rescue-wrapped
   so a missing handler is non-fatal and there is no compile-time event dep."
  [event-type payload]
  (rescue nil
    (when-let [dispatch (requiring-resolve 'hive-mcp.events.core/dispatch)]
      (dispatch [event-type payload]))))

;; -----------------------------------------------------------------------------
;; Policy dispatch
;; -----------------------------------------------------------------------------

(def default-policy
  "Operator-tunable default. `:strategy` may be a single keyword or a vector of
   keywords; a vector is tried left-to-right per failure until one returns
   `:retry`. `:max-attempts` is a positive int."
  {:strategy :throw
   :max-attempts 2})

(defn- apply-single-strategy
  "Run one recovery strategy keyword for a classified failure. Returns `:retry`
   (caller should re-attempt `open-fn`) or `:abort`."
  [strategy classification db-path ex]
  (case strategy
    :throw
    (do (emit! :storage/open-failed
               {:db-path db-path
                :classification classification
                :strategy strategy
                :message (.getMessage ex)})
        :abort)

    :audit
    (do (log/warn "[storage/recovery] Audit mode — would have applied recovery"
                  {:db-path db-path :classification classification})
        (emit! :storage/open-failed
               {:db-path db-path
                :classification classification
                :strategy strategy
                :audit-only true
                :message (.getMessage ex)})
        :abort)

    :truncate
    (if (= :wal-corrupt classification)
      (let [report (truncate-tail! db-path)
            healed (filter :truncated? report)]
        (if (seq healed)
          (do (emit! :storage/wal-truncated
                     {:db-path db-path
                      :classification classification
                      :segments report})
              :retry)
          (do (emit! :storage/open-failed
                     {:db-path db-path
                      :classification classification
                      :strategy strategy
                      :note (if (seq report)
                              "policy=:truncate but no segment had a recoverable tail (mid-segment corruption)"
                              "policy=:truncate but no segments found under txlog/")
                      :segments report
                      :message (.getMessage ex)})
              :abort)))
      (do (emit! :storage/open-failed
                 {:db-path db-path
                  :classification classification
                  :strategy strategy
                  :note "policy=:truncate but classification not :wal-corrupt"
                  :message (.getMessage ex)})
          :abort))

    :quarantine
    (if (= :wal-corrupt classification)
      (let [target (quarantine! db-path)]
        (emit! :storage/wal-quarantined
               {:db-path db-path
                :quarantine-path target
                :classification classification})
        :retry)
      (do (emit! :storage/open-failed
                 {:db-path db-path
                  :classification classification
                  :strategy strategy
                  :note "policy=quarantine but classification not :wal-corrupt"
                  :message (.getMessage ex)})
          :abort))))

(defn- apply-strategy
  "Dispatch `strategy` (keyword or vec of keywords) to the per-failure handler.
   Vector form walks entries left-to-right, returning `:retry` on the first
   success; `:abort` if all abort."
  [strategy classification db-path ex]
  (if (sequential? strategy)
    (reduce
     (fn [_ s]
       (let [outcome (apply-single-strategy s classification db-path ex)]
         (if (= :retry outcome)
           (reduced :retry)
           :abort)))
     :abort
     strategy)
    (apply-single-strategy strategy classification db-path ex)))

;; -----------------------------------------------------------------------------
;; Public composition
;; -----------------------------------------------------------------------------

(defn heal-and-open!
  "Attempt `(open-fn)`. On failure, classify, apply policy, retry up to
   `:max-attempts` total attempts. Returns the conn value or rethrows the last
   exception with `:classification` in its `ex-data`.

   `policy` — see `default-policy`:
     :strategy     keyword | [keyword ...] from #{:throw :audit :truncate :quarantine}
     :max-attempts positive int (default 2)

   `db-path` is the directory `open-fn` opens; a policy may mutate that on-disk
   state before re-invoking `open-fn`. `open-fn` is a 0-arg thunk producing the
   conn — a thunk (not a fixed datalevin call) keeps this ns storage-agnostic."
  [{:keys [policy db-path]} open-fn]
  (let [{:keys [strategy max-attempts]} (merge default-policy policy)]
    (loop [attempt 1
           last-ex nil]
      (if (> attempt max-attempts)
        (throw (ex-info "heal-and-open! exhausted attempts"
                        {:db-path db-path
                         :attempts (dec attempt)
                         :classification (some-> last-ex classify-open-failure)
                         :err :storage/heal-exhausted}
                        last-ex))
        (let [result (try {:ok (open-fn)}
                          (catch Throwable t {:ex t}))]
          (if-let [conn (:ok result)]
            conn
            (let [ex (:ex result)
                  classification (classify-open-failure ex)]
              (log/warn ex "[storage/recovery] Open attempt"
                        attempt "of" max-attempts "failed"
                        {:db-path db-path :classification classification})
              (case (apply-strategy strategy classification db-path ex)
                :retry  (recur (inc attempt) ex)
                :abort  (throw (ex-info "Datalevin open failed and policy aborted retry"
                                        {:db-path db-path
                                         :classification classification
                                         :strategy strategy
                                         :err :storage/open-aborted}
                                        ex))))))))))
