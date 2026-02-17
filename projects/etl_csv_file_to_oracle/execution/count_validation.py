from etl_csv_file_to_oracle.execution.core_engine import read_csv_row_count, pd_read_sql, close_ora_conn, input_file_path, count_compare_dataframes
from etl_csv_file_to_oracle.input.count_validation_target_query import tgt_query
from etl_csv_file_to_oracle.conf.db_conf import ora_engine

source_df = read_csv_row_count(input_file_path)
target_df = pd_read_sql(tgt_query, ora_engine)
print(count_compare_dataframes(source_df, target_df))
close_ora_conn(ora_engine)