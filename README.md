# hive-datalevin

<!-- hive-badges -->

[![Clojars Project](https://img.shields.io/clojars/v/io.github.hive-agi/hive-datalevin.svg)](https://clojars.org/io.github.hive-agi/hive-datalevin)
[![cljdoc](https://cljdoc.org/badge/io.github.hive-agi/hive-datalevin)](https://cljdoc.org/d/io.github.hive-agi/hive-datalevin/CURRENT)
[![release](https://github.com/hive-agi/hive-datalevin/actions/workflows/release.yml/badge.svg)](https://github.com/hive-agi/hive-datalevin/actions/workflows/release.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

<!-- /hive-badges -->

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
