# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 20px;background:linear-gradient(135deg,#1B3139 0%,#2D4A54 100%);border-radius:8px;margin-bottom:8px;">
# MAGIC   <div>
# MAGIC     <span style="color:#fff;font-size:22px;font-weight:700;">設定ノートブック</span><br/>
# MAGIC     <span style="color:rgba(255,255,255,0.7);font-size:13px;">Delta Lake ハンズオン — 00 / 01</span>
# MAGIC   </div>
# MAGIC   <img src="https://cdn.simpleicons.org/databricks/FF3621" width="36" height="36"/>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #607d8b;background:#eceff1;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">⚙️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">設定</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         サーバレスコンピュートで実行してください。<br/>
# MAGIC         <code>catalog_name</code> / <code>schema_name</code> をご自身の環境に合わせて変更してください。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,変数設定
catalog_name = "handson"                     # 任意のカタログ名に変更してください
schema_name = "delta_lake_yourname"          # 任意のスキーマ名に変更してください

# COMMAND ----------

# DBTITLE 1,リセット用（必要な場合のみコメント解除）
# spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_name} CASCADE")

# COMMAND ----------

# DBTITLE 1,カタログ・スキーマ作成
# spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name};")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name};")

spark.sql(f"USE CATALOG {catalog_name};")
spark.sql(f"USE SCHEMA {schema_name};")

# COMMAND ----------

# DBTITLE 1,設定内容の表示
print(f"catalog_name: {catalog_name}")
print(f"schema_name:  {schema_name}")
print(f"実行済み: USE CATALOG {catalog_name}")
print(f"実行済み: USE SCHEMA {schema_name}")
