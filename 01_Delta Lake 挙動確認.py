# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md-sandbox
# MAGIC <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 20px;background:linear-gradient(135deg,#1B3139 0%,#2D4A54 100%);border-radius:8px;margin-bottom:8px;">
# MAGIC   <div>
# MAGIC     <div style="display:flex;align-items:center;gap:10px;">
# MAGIC       <span style="color:#fff;font-size:22px;font-weight:700;">01 | Delta Lake 挙動確認</span>
# MAGIC       <span style="background:#e3f2fd;color:#1565c0;padding:3px 10px;border-radius:4px;font-size:12px;font-weight:600;">⏱ 30分</span>
# MAGIC     </div>
# MAGIC     <span style="color:rgba(255,255,255,0.7);font-size:13px;">Delta Lake ハンズオン — 01 / 01</span>
# MAGIC   </div>
# MAGIC   <img src="https://cdn.simpleicons.org/databricks/FF3621" width="36" height="36"/>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #ffc107;background:#fffde7;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">🎯</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">このノートブックのゴール</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         Delta Lake の基本的な仕組みを理解するために、実際にテーブルを操作しながら以下を確認します。
# MAGIC         <ul style="margin:8px 0 0;padding-left:20px;">
# MAGIC           <li>Delta Table の物理構造（Parquet + <code>_delta_log</code>）</li>
# MAGIC           <li>トランザクションログの役割と変化</li>
# MAGIC           <li>INSERT / UPDATE / DELETE 時のバージョン管理</li>
# MAGIC           <li>Time Travel による過去データの参照と RESTORE による復元</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. 環境セットアップ

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

spark.sql('SELECT current_catalog(), current_schema()').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. サンプルデータの生成

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">ℹ️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">データ概要</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         ホームセンターの店舗売上データを模したサンプル（約100万行）を生成します。<br/>
# MAGIC         複数の Parquet ファイルが作られるよう、十分なデータ量にしています。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, rand, floor, date_add, to_date,
    concat, lpad, when, round as spark_round
)

num_stores = 50
num_products = 10000
num_days = 365 * 3
records_per_day = 1000
total_records = num_days * records_per_day

print(f"生成予定レコード数: {total_records:,} 件")

# COMMAND ----------

base_date = "2023-01-01"

df_sales = (
    spark.range(0, total_records)
    .withColumn("store_id", (floor(rand() * num_stores) + 1).cast("int"))
    .withColumn("product_id", (floor(rand() * num_products) + 1).cast("int"))
    .withColumn("day_offset", (floor(rand() * num_days)).cast("int"))
    .withColumn("sale_date", date_add(to_date(lit(base_date)), col("day_offset")))
    .withColumn("quantity", (floor(rand() * 10) + 1).cast("int"))
    .withColumn("unit_price", spark_round(rand() * 5000 + 100, 0).cast("int"))
    .withColumn("sales_amount", col("quantity") * col("unit_price"))
    .withColumn("store_name", concat(lit("店舗_"), lpad(col("store_id").cast("string"), 3, "0")))
    .withColumn("product_category",
        when(col("product_id") % 10 == 0, "園芸用品")
        .when(col("product_id") % 10 == 1, "工具")
        .when(col("product_id") % 10 == 2, "塗料")
        .when(col("product_id") % 10 == 3, "木材")
        .when(col("product_id") % 10 == 4, "金物")
        .when(col("product_id") % 10 == 5, "電材")
        .when(col("product_id") % 10 == 6, "水道用品")
        .when(col("product_id") % 10 == 7, "収納用品")
        .when(col("product_id") % 10 == 8, "インテリア")
        .otherwise("日用品")
    )
    .withColumn("prefecture",
        when(col("store_id") <= 10, "北海道")
        .when(col("store_id") <= 20, "東北")
        .when(col("store_id") <= 30, "関東")
        .when(col("store_id") <= 40, "中部")
        .otherwise("関西")
    )
    .select(
        "store_id", "store_name", "prefecture",
        "product_id", "product_category",
        "sale_date", "quantity", "unit_price", "sales_amount"
    )
)

display(df_sales.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Delta Table として保存

# COMMAND ----------

(
    df_sales
    .repartition(10)
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("store_sales")
)

print(f"テーブル {catalog_name}.{schema_name}.store_sales を作成しました")
print(f"レコード数: {spark.table('store_sales').count():,} 件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Delta Table の構造を理解する

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">ℹ️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">Delta Lake の物理構造</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         Delta Table は <strong>Parquet ファイル</strong>（実データ）と <strong>_delta_log</strong>（トランザクションログ）で構成されています。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="background:#1e1e1e;border-radius:8px;padding:16px 20px;margin:12px 0;overflow-x:auto;">
# MAGIC   <pre style="margin:0;color:#d4d4d4;font-family:'Fira Code','Consolas',monospace;font-size:13px;line-height:1.5;"><code>テーブルの保存場所/
# MAGIC ├── _delta_log/                    ← トランザクションログ（JSON）
# MAGIC │   ├── 00000000000000000000.json  ← バージョン 0（CREATE TABLE）
# MAGIC │   ├── 00000000000000000001.json  ← バージョン 1（INSERT / UPDATE 等）
# MAGIC │   └── ...
# MAGIC ├── part-00000-xxx.parquet         ← 実データ（列指向）
# MAGIC ├── part-00001-xxx.parquet
# MAGIC └── ...</code></pre>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 テーブルの詳細情報

# COMMAND ----------

# DBTITLE 1,DESCRIBE DETAIL — テーブルメタデータ
display(spark.sql("DESCRIBE DETAIL store_sales"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #009688;background:#e0f2f1;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">💡</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">DESCRIBE DETAIL の主な項目</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         <ul style="margin:4px 0 0;padding-left:20px;">
# MAGIC           <li><strong>format</strong>: テーブルフォーマット（delta）</li>
# MAGIC           <li><strong>location</strong>: データの物理的な保存場所（S3/ADLS/GCS）</li>
# MAGIC           <li><strong>numFiles</strong>: 現在有効な Parquet ファイル数</li>
# MAGIC           <li><strong>sizeInBytes</strong>: データサイズ</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,DESCRIBE EXTENDED — スキーマとプロパティ
display(spark.sql("DESCRIBE EXTENDED store_sales"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 トランザクションログ（変更履歴）

# COMMAND ----------

# DBTITLE 1,DESCRIBE HISTORY — 操作履歴
display(spark.sql("DESCRIBE HISTORY store_sales"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #009688;background:#e0f2f1;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">💡</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">DESCRIBE HISTORY の見方</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         <ul style="margin:4px 0 0;padding-left:20px;">
# MAGIC           <li><strong>version</strong>: トランザクションの連番（0 から開始）</li>
# MAGIC           <li><strong>operation</strong>: 操作の種類（CREATE TABLE, WRITE, MERGE, DELETE など）</li>
# MAGIC           <li><strong>operationMetrics</strong>: 追加・削除されたファイル数、行数</li>
# MAGIC           <li><strong>userName</strong>: 誰が操作したか</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. データを更新してバージョンの変化を観察する

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 INSERT — データ追加

# COMMAND ----------

spark.sql("""
    INSERT INTO store_sales
    VALUES (99, '店舗_テスト', 'テスト地域', 99999, '特別商品', current_date(), 100, 9999, 999900)
""")
print("テストデータを 1 件追加しました")

# COMMAND ----------

# DBTITLE 1,INSERT 後の履歴を確認
display(spark.sql("DESCRIBE HISTORY store_sales"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #4caf50;background:#e8f5e9;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">📝</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">確認ポイント</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         新しいバージョンが追加され、<code>operation = WRITE</code>、<code>operationMetrics</code> に追加ファイル数と行数が記録されていることを確認してください。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 UPDATE — データ更新

# COMMAND ----------

spark.sql("""
    UPDATE store_sales
    SET sales_amount = sales_amount * 1.1
    WHERE prefecture = '北海道' AND sale_date = '2023-01-01'
""")
print("北海道・2023-01-01 の売上を 10% 増に更新しました")

# COMMAND ----------

# DBTITLE 1,UPDATE 後の履歴を確認
display(spark.sql("DESCRIBE HISTORY store_sales"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #4caf50;background:#e8f5e9;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">📝</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">確認ポイント</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         <code>operation = UPDATE</code> で新バージョンが作られています。<br/>
# MAGIC         Delta Lake は更新対象のファイルを「削除 → 新ファイル追加」で処理します（<strong>Copy-on-Write</strong>）。<br/>
# MAGIC         <code>operationMetrics</code> の <code>numRemovedFiles</code> / <code>numAddedFiles</code> でその挙動を確認できます。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 DELETE — データ削除

# COMMAND ----------

spark.sql("""
    DELETE FROM store_sales WHERE store_id = 99
""")
print("テストデータ（store_id = 99）を削除しました")

# COMMAND ----------

# DBTITLE 1,DELETE 後の履歴を確認
display(spark.sql("DESCRIBE HISTORY store_sales"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #4caf50;background:#e8f5e9;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">📝</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">確認ポイント</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         <code>operation = DELETE</code>。UPDATE と同様に Copy-on-Write が行われます。<br/>
# MAGIC         削除されたデータは物理的にはまだ残っており、<strong>Time Travel で参照できます</strong>。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 ファイル数・サイズの変化を確認

# COMMAND ----------

# DBTITLE 1,現在の numFiles / sizeInBytes
display(spark.sql("DESCRIBE DETAIL store_sales").select("numFiles", "sizeInBytes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Time Travel — 過去のデータにアクセスする

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">ℹ️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">Time Travel とは</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         Delta Lake はすべての変更履歴を保持しているため、過去の任意のバージョンのデータを参照できます。<br/>
# MAGIC         「加工を間違えた」「うっかりデータを消した」場合でも安心です。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 バージョン指定で過去データを参照

# COMMAND ----------

v0_count = spark.sql("SELECT COUNT(*) AS cnt FROM store_sales VERSION AS OF 0").collect()[0]["cnt"]
current_count = spark.table("store_sales").count()

print(f"バージョン 0（初期作成時）: {v0_count:,} 件")
print(f"現在の最新バージョン:       {current_count:,} 件")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 削除したデータが過去バージョンに残っていることを確認

# COMMAND ----------

# DBTITLE 1,DELETE 前のバージョンからテストデータを検索
display(spark.sql("""
    SELECT * FROM store_sales VERSION AS OF 1
    WHERE store_id = 99
"""))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #4caf50;background:#e8f5e9;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">📝</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">確認ポイント</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         DELETE で消したはずの <code>store_id = 99</code> のデータが、バージョン 1（INSERT 直後）には存在しています。<br/>
# MAGIC         これが Time Travel — 過去の任意の時点のデータに自由にアクセスできます。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 RESTORE — テーブルを過去バージョンに復元

# COMMAND ----------

import time

def retry_delta(sql_text, max_retries=3, delay=5):
    """自動最適化（Auto-OPTIMIZE）との競合時にリトライする"""
    for attempt in range(max_retries):
        try:
            return spark.sql(sql_text)
        except Exception as e:
            if "DELTA_CONCURRENT_WRITE" in str(e) and attempt < max_retries - 1:
                print(f"Auto-OPTIMIZE と競合 — {delay}秒後にリトライ ({attempt+1}/{max_retries})")
                time.sleep(delay)
            else:
                raise

# COMMAND ----------

# DBTITLE 1,バージョン 0 に復元
retry_delta("RESTORE TABLE store_sales TO VERSION AS OF 0")
restored_count = spark.table("store_sales").count()
print(f"復元後レコード数: {restored_count:,} 件（= バージョン 0 と同じ）")

# COMMAND ----------

# DBTITLE 1,RESTORE 後の履歴
display(spark.sql("DESCRIBE HISTORY store_sales"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #4caf50;background:#e8f5e9;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">📝</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">確認ポイント</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         RESTORE 自体も新しいバージョンとして記録されます。<br/>
# MAGIC         「元に戻した」という操作もトランザクションログに残るため、完全な監査証跡になります。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. まとめ

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <table style="width:100%;border-collapse:collapse;margin:16px 0;font-size:14px;">
# MAGIC   <thead>
# MAGIC     <tr style="background:#1B3139;color:#fff;">
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;">機能</th>
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;">概要</th>
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;">確認したコマンド</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background:#fff;">
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-weight:600;">ACID トランザクション</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">全ての操作がアトミックに記録される</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;"><code>DESCRIBE HISTORY</code></td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F8F9FA;">
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-weight:600;">Time Travel</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">過去の任意のバージョンのデータを参照可能</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;"><code>VERSION AS OF N</code></td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#fff;">
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-weight:600;">RESTORE</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">テーブルを過去の状態に復元</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;"><code>RESTORE TABLE ... TO VERSION AS OF N</code></td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F8F9FA;">
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-weight:600;">Copy-on-Write</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">UPDATE/DELETE は古いファイルを残したまま新ファイルを作成</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;"><code>operationMetrics</code></td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. クリーンアップ（オプション）

# COMMAND ----------

# テーブルを削除（コメントアウト解除で実行）
# spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.store_sales")
# print("テーブル store_sales を削除しました")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #4caf50;background:#e8f5e9;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">✅</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">Delta Lake 挙動確認 完了</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         Delta Table の構造、トランザクションログ、INSERT / UPDATE / DELETE のバージョン管理、Time Travel / RESTORE を体験しました。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
