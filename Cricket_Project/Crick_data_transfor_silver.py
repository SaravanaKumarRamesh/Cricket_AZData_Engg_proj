# Databricks notebook source
# MAGIC %run ./Crick_proj_config

# COMMAND ----------

Crick_brnz_raw_df_1 = spark.read.format("delta").load("dbfs:/user/hive/warehouse/brnze_cric_dl_db.db/brnz_cric_tbl")

# COMMAND ----------

info_cols = [c for c in Crick_brnz_raw_df.columns if "info_" in c and "info_registry_" not in c and "info_players_" not in c]+["Match_id"]
inn_cols = [c for c in Crick_brnz_raw_df.columns if "innings_" in c and "innings_overs_deliveries_replacements_" not in c and "innings_miscounted_overs_" not in c]+["Match_id"]

# COMMAND ----------

# MAGIC %md ##INFO DF##

# COMMAND ----------

# Renaming the info columns 
Crick_info_raw_df = Crick_brnz_raw_df.select(*info_cols)
new_column_names_info = [re.sub("(info_*)","",Crick_info_raw_df.columns[i]) for i in range(len(Crick_info_raw_df.columns))]
crick_info_renamecols_df = Crick_info_raw_df.toDF(*new_column_names_info)


# COMMAND ----------

# transposing umpires and rows from rows to columns
Col_div_df = crick_info_renamecols_df.groupby(['Match_id'])\
                            .agg(collect_set(col("officials_umpires")).alias("umpires"),collect_set(col("teams")).alias("teams_t"))\
                            .select("Match_id",col("umpires")[0].alias("umpire_1"),col("umpires")[1].alias("umpire_2"),col("teams_t")[0].alias("Team_A"),col("teams_t")[1].alias("Team_B"))         

crick_info_final_df = crick_info_renamecols_df.join(Col_div_df,on=['Match_id'],how="inner")\
                                        .withColumn("season",year(col("dates")))\
                                        .drop("officials_umpires","teams")\
                                        .dropDuplicates()

# COMMAND ----------

# MAGIC %md ##Innings DF##

# COMMAND ----------

Crick_innings_raw_df = Crick_brnz_raw_df.select(*inn_cols)
new_column_names_inn = [re.sub("(innings_*)|(overs_)|(deliveries_*)|(wickets_*)","",Crick_innings_raw_df.columns[i]) for i in range(len(Crick_innings_raw_df.columns))]
crick_innings_final_df = Crick_innings_raw_df.toDF(*new_column_names_inn)


# COMMAND ----------

# MAGIC %md ##Joined DF

# COMMAND ----------

crick_join_df = crick_innings_final_df.join(crick_info_final_df,on=['Match_id'],how="inner")\
                                             .orderBy(['Match_id','team','over'])

# COMMAND ----------

crick_filled_df = crick_join_df.na.fill({ 'extras_byes':0, 'extras_legbyes':0,'extras_noballs':0,'extras_penalty':0,'extras_wides':0,'event_stage':"League" })

# COMMAND ----------

display(crick_filled_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC --creating the database
# MAGIC CREATE database if not exists  Silver_cric_dl_db

# COMMAND ----------

crick_filled_df.write.partitionBy("season").format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("Silver_cric_dl_db.Silver_cric_tbl")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DATABASE Silver_cric_dl_db

# COMMAND ----------


