# Delta Lake 挙動確認 ハンズオン

## 概要

Delta Lake の基本的な仕組みを、実際にテーブルを操作しながら体験するハンズオンです。

## 技術スタック

| カテゴリ | 技術 |
|---|---|
| コンピュート | Serverless |
| 言語 | Python (PySpark), SQL |
| テーブルフォーマット | Delta Lake |
| データガバナンス | Unity Catalog |

## 対象

- Databricks 初学者（Delta Lake の仕組みを知りたい方）
- 約30分

## ノートブック構成

| # | ノートブック | 時間 | 内容 |
|---|---|---|---|
| 00 | config | — | カタログ・スキーマの変数定義とセットアップ |
| 01 | Delta Lake 挙動確認 | 30分 | サンプルデータ生成、Delta Table 構造確認、INSERT/UPDATE/DELETE、Time Travel、RESTORE |

## 学習内容

- **Delta Table の物理構造**: Parquet ファイル + `_delta_log`（トランザクションログ）
- **トランザクションログ**: `DESCRIBE DETAIL` / `DESCRIBE HISTORY` による確認
- **バージョン管理**: INSERT / UPDATE / DELETE 時の Copy-on-Write 挙動
- **Time Travel**: `VERSION AS OF` による過去データの参照
- **RESTORE**: テーブルを過去バージョンに復元

## 事前準備

1. `00_config` の `catalog_name` / `schema_name` をご自身の環境に合わせて変更
2. サーバレスコンピュートで実行
