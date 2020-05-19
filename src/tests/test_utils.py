from src import utils


def test_utils(spark_session):
	spark_session.sql("CREATE DATABASE IF NOT EXISTS sample")
	input_df = spark_session.createDataFrame([[1, 's1'], [2, 's2'], [3, 's3']], "id: integer, text:string")
	utils.create_table(spark_session, input_df, "sample", "tbl")
	sql = "SELECT * FROM sample.tbl"
	output_df = utils.execute(spark_session, sql)
	assert output_df.count() == input_df.count()
	sql = "SELECT * FROM sample.tbl WHERE id=1"
	actual_df = utils.execute(spark_session, sql)
	expected_df = spark_session.createDataFrame([[1, 's1']], "id: integer, text:string")
	assert actual_df == expected_df