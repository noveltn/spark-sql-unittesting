def create_table(spark, sdf, schm_nm, tbl_nm):
	sdf.createOrReplaceTempView("sdf")
	spark.sql(f"DROP TABLE IF EXISTS {schm_nm}.{tbl_nm}")
	spark.sql(f"CREATE TABLE {schm_nm}.{tbl_nm} as SELECT * FROM sdf")
	return 1
	
	
def execute(spark, sql):
	return spark.sql(sql)
	