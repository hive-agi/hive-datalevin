# hive-datalevin

Datalevin (LMDB) implementation of the hive KG storage SPI.

`src/hive_datalevin/kg/store.clj` implements
`hive-spi.kg.protocol/IKGStore` + `IPersistentKGStore` on top of
[datalevin](https://github.com/juji-io/datalevin). It depends only on
`hive-spi` (protocol contracts) and `hive-dsl` — **not** on hive-mcp.
Domain schema, the DataScript→Datalevin value-type map, `db-path`, and
`cache-limit` are all **injected by the host** at store-construction time,
so the backend stays free of any application schema or config resolution.

`src/hive_datalevin/kg/recovery.clj` provides LMDB open-failure
classification and a configurable self-heal policy
(`:throw | :audit | :truncate | :quarantine`) composed by `heal-and-open!`.

## Use

```clojure
(require '[hive-datalevin.kg.store :as store])

(def s (store/create-store
        {:db-path        "/path/to/db"
         :base-schema    my-datascript-schema   ; injected by host
         :extra-schema   per-store-additions
         :recovery-policy {:strategy :throw}}))

(require '[hive-spi.kg.protocol :as kg])
(kg/transact! s [...])
(kg/query s '[:find ?e :where [?e :kg-edge/id _]])
```

## Layout

```
hive-datalevin/
├── deps.edn
├── .hive-project.edn
├── src/hive_datalevin/kg/store.clj
└── src/hive_datalevin/kg/recovery.clj
```
