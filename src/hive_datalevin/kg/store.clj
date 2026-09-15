(ns hive-datalevin.kg.store
  "Datalevin (LMDB) implementation of hive-spi.kg.protocol/IKGStore. Schema,
   value-type map, db-path and cache-limit are injected by the host — the store
   carries no domain schema or config resolution of its own."
  (:require [datalevin.core :as dtlv]
            [hive-spi.kg.protocol :as kg]
            [hive-spi.kg.conn-init :as ci]
            [hive-datalevin.kg.recovery :as rec]
            [hive-dsl.result :refer [rescue]]
            [datalevin.db :as dtlv-db]
            [datalevin.interface :as dtlv-i]
            [clojure.java.io :as io]
            [taoensso.timbre :as log])
  (:import [datalevin.db DB]))
;; SPDX-License-Identifier: MIT

(def default-value-type-map
  "DataScript attribute -> Datalevin :db/valueType. The host may override or
   extend via create-store's :value-type-map. Attributes absent here keep the
   translated schema's inferred type."
  {:kg-edge/id            :db.type/string
   :kg-edge/from          :db.type/string
   :kg-edge/to            :db.type/string
   :kg-edge/relation      :db.type/keyword
   :kg-edge/scope         :db.type/string
   :kg-edge/confidence    :db.type/double
   :kg-edge/created-by    :db.type/string
   :kg-edge/created-at    :db.type/instant
   :kg-edge/last-verified :db.type/instant
   :kg-edge/source-type   :db.type/keyword
   :kg-edge/schema-text    :db.type/string
   :kg-edge/schema-head    :db.type/string
   :kg-edge/schema-subject :db.type/string

   :knowledge/abstraction-level :db.type/long
   :knowledge/grounded-at       :db.type/instant
   :knowledge/grounded-from     :db.type/string
   :knowledge/gaps              :db.type/keyword
   :knowledge/source-hash       :db.type/string
   :knowledge/source-type       :db.type/keyword

   :disc/path         :db.type/string
   :disc/content-hash :db.type/string
   :disc/analyzed-at  :db.type/instant
   :disc/git-commit   :db.type/string
   :disc/project-id   :db.type/string
   :disc/last-read-at :db.type/instant
   :disc/read-count   :db.type/long
   :disc/certainty-alpha  :db.type/double
   :disc/certainty-beta   :db.type/double
   :disc/volatility-class :db.type/keyword
   :disc/last-observation :db.type/instant})

(defn translate-schema
  "Translate a DataScript-shaped schema to a Datalevin schema, stamping
   :db/valueType from `value-type-map` where present and dropping :db/doc."
  [ds-schema value-type-map]
  (reduce-kv
   (fn [acc attr props]
     (let [clean-props (dissoc props :db/doc)
           typed-props (if-let [vt (get value-type-map attr)]
                         (assoc clean-props :db/valueType vt)
                         clean-props)]
       (assoc acc attr typed-props)))
   {}
   ds-schema))

(defn- validate-db-path!
  "Validate `db-path` is non-empty and ensure its parent directory exists."
  [db-path]
  (when (or (nil? db-path) (empty? db-path))
    (throw (ex-info "Datalevin db-path cannot be nil or empty"
                    {:db-path db-path})))
  (let [dir (io/file db-path)]
    (when-not (.exists (.getParentFile dir))
      (log/info "Creating Datalevin parent directory" {:path (.getParent dir)})
      (.mkdirs (.getParentFile dir))))
  db-path)

(defn- open-conn!
  "Open (heal-and-open) the datalevin conn for `store`'s configuration.
   Factored from ensure-conn! so liveness recovery can rebuild the conn
   through the same recovery-policy path.

   `conn-opts` is forwarded verbatim to `datalevin.core/get-conn` after the
   host-level keys (`:cache-limit`), so a caller can set any datalevin db
   option (`:background-sampling?`, `:kv-opts`, ...) without this namespace
   naming each one."
  [{:keys [db-path base-schema extra-schema value-type-map
           recovery-policy cache-limit conn-opts]}]
  (log/info "Initializing Datalevin KG store"
            {:path db-path
             :recovery-strategy (:strategy recovery-policy)})
  (validate-db-path! db-path)
  (let [vtm           (or value-type-map default-value-type-map)
        base          (translate-schema (or base-schema {}) vtm)
        merged-schema (if extra-schema
                        (merge base (translate-schema extra-schema vtm))
                        base)
        conn-opts*    (cond-> (or conn-opts {})
                        (some? cache-limit) (assoc :cache-limit cache-limit))]
    (log/debug "Datalevin schema translated"
               {:attributes (count merged-schema)
                :extra-attributes (when extra-schema (count extra-schema))
                :cache-limit cache-limit
                :conn-opts conn-opts*})
    (rec/heal-and-open!
     {:policy recovery-policy :db-path db-path}
     #(dtlv/get-conn db-path merged-schema conn-opts*))))

(defn- close-conn!
  "Close datalevin `conn`, then drop the store's entry from datalevin.db's
   process-global read-cache map once the underlying store reports closed.
   A store still shared by another live conn on the same directory keeps its
   cache. Never throws; returns nil."
  [conn]
  (let [st (rescue nil (.-store ^DB @conn))]
    (rescue nil (dtlv/close conn))
    (when (and st (rescue false (dtlv-i/closed? st)))
      (rescue nil (dtlv-db/remove-cache st)))
    nil))

(defrecord DatalevinStore [conn-init db-path base-schema extra-schema
                           value-type-map recovery-policy cache-limit
                           conn-opts]
  kg/IKGStore

  (ensure-conn! [this]
    ;; Single-init via IConnInit: concurrent callers block on the first open
    ;; and observe the cached conn, avoiding the LMDB file-lock race. A cached
    ;; conn that reports closed (closed behind the store's back) is discarded
    ;; and reopened — a dead conn is never returned.
    (let [conn (ci/open-once! conn-init #(open-conn! this))]
      (if (and conn (rescue false (dtlv/closed? conn)))
        (do (log/warn "Datalevin conn found closed — reopening"
                      {:path db-path})
            (ci/clear! conn-init)
            (ci/open-once! conn-init #(open-conn! this)))
        conn)))

  (transact! [this tx-data]
    ;; Heal-once on a dead conn (closed env / closed channel), then retry.
    ;; Any other failure — schema, corruption, domain — surfaces unchanged.
    (let [conn (kg/ensure-conn! this)]
      (try
        (dtlv/transact! conn tx-data)
        (catch Throwable e
          (if (or (rescue false (dtlv/closed? conn))
                  (rec/dead-conn-throwable? e))
            (do (log/warn "Datalevin transact! hit a dead conn — healing and retrying once"
                          {:path db-path :error (ex-message e)})
                (dtlv/transact! (kg/reset-conn! this) tx-data))
            (throw e))))))

  (query [this q]
    (dtlv/q q (dtlv/db (kg/ensure-conn! this))))

  (query [this q inputs]
    (apply dtlv/q q (dtlv/db (kg/ensure-conn! this)) inputs))

  (entity [this eid]
    (dtlv/entity (dtlv/db (kg/ensure-conn! this)) eid))

  (entid [this lookup-ref]
    (dtlv/entid (dtlv/db (kg/ensure-conn! this)) lookup-ref))

  (pull-entity [this pattern eid]
    (dtlv/pull (dtlv/db (kg/ensure-conn! this)) pattern eid))

  (eids-by-attr [this attr]
    ;; :ave index iterates attribute-first over LMDB; consuming the datom seq
    ;; lazily keeps memory bounded.
    (map :e (dtlv/datoms (dtlv/db (kg/ensure-conn! this)) :ave attr)))

  (db-snapshot [this]
    (dtlv/db (kg/ensure-conn! this)))

  (reset-conn! [this]
    ;; NON-DESTRUCTIVE — close and reopen the SAME on-disk DB.
    (log/info "Reopening Datalevin KG store (non-destructive)" {:path db-path})
    (when-let [c (ci/snapshot conn-init)]
      (close-conn! c))
    (ci/clear! conn-init)
    (kg/ensure-conn! this))

  (close! [_this]
    (when-let [c (ci/snapshot conn-init)]
      (log/info "Closing Datalevin KG store" {:path db-path})
      (close-conn! c)
      (ci/clear! conn-init)))

  kg/IPersistentKGStore

  (delete-database! [_this confirm]
    ;; DESTRUCTIVE — guard required. confirm must be :i-mean-it.
    (when-not (= confirm :i-mean-it)
      (throw (ex-info "delete-database! requires confirm=:i-mean-it"
                      {:passed-confirm confirm
                       :hint "This call deletes the database directory from disk. Pass :i-mean-it explicitly to proceed."
                       :backend :datalevin
                       :db-path db-path})))
    (log/error "[storage/destruction-fired] Datalevin delete-database! invoked"
               {:backend :datalevin
                :db-path db-path
                :stacktrace (mapv str (.getStackTrace (Throwable.)))})
    (when-let [c (ci/snapshot conn-init)]
      (close-conn! c))
    (let [dir (io/file db-path)]
      (when (.exists dir)
        (doseq [f (reverse (file-seq dir))]
          (.delete f))))
    (ci/clear! conn-init)
    (log/error "[storage/destruction-completed] Datalevin directory deleted"
               {:backend :datalevin :db-path db-path})
    nil))

(defn create-store
  "Create a Datalevin-backed IKGStore.

   opts:
     :db-path         (required) on-disk LMDB directory
     :base-schema     DataScript-shaped KG schema map, injected by the host
     :extra-schema    per-store schema additions, merged over base-schema
     :value-type-map  attr -> :db/valueType overrides (default
                      `default-value-type-map`)
     :cache-limit     bounds the Datalog index-cache LRU forwarded to get-conn;
                      nil leaves upstream behaviour untouched
     :conn-opts       map forwarded verbatim to `datalevin.core/get-conn`
                      (e.g. {:background-sampling? false}); :cache-limit
                      above is layered on top of it
     :recovery-policy forwarded to `recovery/heal-and-open!`
                      {:strategy :throw|:audit|:truncate|:quarantine|[..]
                       :max-attempts pos-int}

   Config/schema resolution is the host's responsibility — this fn does not read
   env or config.edn. Returns nil on construction failure (rescue-wrapped)."
  [& [{:keys [db-path base-schema extra-schema value-type-map
              recovery-policy cache-limit conn-opts]}]]
  (rescue nil
          (do
            (log/info "Creating Datalevin graph store"
                      {:path db-path
                       :extra-schema? (some? extra-schema)
                       :cache-limit cache-limit
                       :conn-opts conn-opts
                       :recovery-strategy (:strategy recovery-policy)})
            (map->DatalevinStore {:conn-init       (ci/atom-conn-init)
                                  :db-path         db-path
                                  :base-schema     base-schema
                                  :extra-schema    extra-schema
                                  :value-type-map  value-type-map
                                  :recovery-policy recovery-policy
                                  :cache-limit     cache-limit
                                  :conn-opts       conn-opts}))))