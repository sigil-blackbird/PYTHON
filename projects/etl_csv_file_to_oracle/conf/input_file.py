from etl_csv_file_to_oracle.conf.proj_conf import get_input_path

input_file_name = 'taxi_trip_data.csv'
input_file_path = f"{get_input_path()}/{input_file_name}"
desired_columns = ['pick_up_time', 'drop_off_time', 'trip_distance', 'trip_fare', 'payment_method', 'cab_color', 'pickup_location', 'pickup_zone', 'dropoff_location', 'dropoff_zone']