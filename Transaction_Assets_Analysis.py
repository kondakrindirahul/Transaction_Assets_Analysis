
# coding: utf-8

# # Required packages for the Script
# 

# In[1]:

# data analysis and wrangling
import pandas as pd
import numpy as np
from geopy.distance import vincenty
import math

# visualization
import seaborn as sns
import matplotlib.pyplot as plt
import gmplot

# database connection
import pymysql.cursors
import pandas
from pandas.io import sql

# readme text embedded in the script
import argparse

parser = argparse.ArgumentParser(description='Enter the queries to obtain global_table_track_records and global_table_transaction_records.Filter the transaction records which have vehicle registration number as 0 as these are test transactions. Pick out a single transaction and identify its gps coordinates. This will be our plant location. Take a fixed time period before and after the transaction device time stamp and filter the track records to obtain only those track records that are occuring within the said time frame. Now fix the radius of the plant and further filter the track records to include only those track records whose location is within the plant fence. From these track records, identify the unique packet_generating_station_ids which will be our unique assets near that plant during the particular transaction. Now find the gps locations of these unique assets within the time frame which has been extended for a further fixed time that you will be asked to provide. Plot the path travelled by each asset on google maps which will be used for visual validation.')

args = parser.parse_args()

# # The Class Analyzer takes the following inputs.
# ## (i)   query for obtaining global table transaction records for a day
# ## (ii)  query for obtaining global table track records for a day
# ## (iii) the transaction record id of a particular transaction
# ## (iv) bounding time before and after the device time stamp of the transaction
# ## (v)  the radius of the plant fence
# ## (vi) the extending time period for tracking the path of unique assets 

# In[2]:

class Analyzer(object):
    
    def __init__(self, connection):
        
        self.connection = connection
        
    def runAnalysis(self):
        
        # entering the sql queries for transaction and track records 
        transaction_query = input("Enter the transaction query : ")
        track_query = input("Enter the track query : ")
        transaction_id = int(input("Enter the transaction_record_id : "))
        upper_bound = input("Enter the bounding time : ")
        fence_radius = input("Enter the fence radius : ")
        extended_time = input("Enter the extended amount of time : ")


        # converting the sql data into pandas dataframes
        transaction_df = sql.read_sql(transaction_query, con=connection)
        track_df = sql.read_sql(track_query, con=connection)
        
        # modifying the transaction_records dataframe 
        drop_column_list = ['transaction_info', 'material_net_volume', 'transaction_packet_id', 'packet_type', 'status_code', 'time_stamp_gps', 'distance_gps', 'packet_sequence', 'hop_count', 'hop_1_info', 'hop_2_info', 'hop_3_info', 'hop_4_info', 'packet_info', 'operator_id', 'transaction_reference', 'material_invoice_weight', 'material_invoice_volume', 'material_net_weight', 'tare_weight', 'gross_weight', 'time_stamp_asset', 'transaction_link_packet_id', 'total_batches', 'component1_code', 'component1_qty', 'component2_code', 'component2_qty', 'component3_code', 'component3_qty', 'component4_code', 'component4_qty', 'component5_code', 'component5_qty', 'component6_code', 'component6_qty', 'component7_code', 'component7_qty', 'component8_code', 'component8_qty', 'component9_code', 'component9_qty', 'raw_transaction_out_packet', 'server_time_stamp']
        transaction_df.drop(drop_column_list, axis=1, inplace=True)
        modified_transaction_df = transaction_df.loc[transaction_df['vehical_registration_number'] != '000000000000']
        
        # obtaining the gps location and device time stamp of a particular transaction
        plant_latitude_series = modified_transaction_df.loc[modified_transaction_df['transaction_record_id'] == transaction_id, 'latitude_gps']
        plant_lat_list = plant_latitude_series.tolist()
        plant_latitude = plant_lat_list[0]
        plant_longitude_series = modified_transaction_df.loc[modified_transaction_df['transaction_record_id'] == transaction_id, 'longitude__gps']
        plant_long_list = plant_longitude_series.tolist()
        plant_longitude = plant_long_list[0]
        device_time_series = modified_transaction_df.loc[modified_transaction_df['transaction_record_id'] == transaction_id, 'device_time_stamp']
        device_time_list = device_time_series.tolist()
        device_time = device_time_list[0]
        plant_device_time = str(device_time)
        
        # adding a lower bound and an upper bound to the the plant_device_time
        lower_bound = "-" + upper_bound
        lower_bound_time_series = pd.date_range(plant_device_time, periods=2, freq=lower_bound)
        upper_bound_time_series = pd.date_range(plant_device_time, periods=2, freq=upper_bound)
        lower_bound_time = lower_bound_time_series.tolist()[1]
        upper_bound_time = upper_bound_time_series.tolist()[1]
        
        # obtaining the track records present in the bounding time period
        filtered_track_df = (track_df['device_time_stamp'] > lower_bound_time) & (track_df['device_time_stamp'] <= upper_bound_time)
        time_bounded_track_df = track_df.loc[filtered_track_df]
        
        # filter the track records and obtain the track records which lie within the radius of the plant
        plant_gps = (plant_latitude, plant_longitude)

        for i in time_bounded_track_df.index:
            track_gps = (time_bounded_track_df.loc[i].latitude_gps, time_bounded_track_df.loc[i].longitude_gps)
            distance = vincenty(plant_gps, track_gps).meters
            time_bounded_track_df.loc[i,'distances'] = distance
            
        distance_bounded_track_df = time_bounded_track_df.loc[time_bounded_track_df['distances'] <= int(fence_radius)]
        
        # creating a list that contains the unique packet generation station ids
        packet_id_series = distance_bounded_track_df.packet_generating_station_id
        packet_id_list = packet_id_series.tolist()
        packet_id = set(packet_id_list)
        asset_id = list(packet_id)
        asset_count = len(asset_id)
        
        # obtain the gps locations of these unique packet generation station ids
        asset_gps_locations = {}
        lat = []
        long = []
        asset_lat_names = []
        asset_long_names = []

        for each in asset_id:

            latitude_series = distance_bounded_track_df.loc[distance_bounded_track_df['packet_generating_station_id'] == each, 'latitude_gps']
            longitude_series = distance_bounded_track_df.loc[distance_bounded_track_df['packet_generating_station_id'] == each, 'longitude_gps']

            lat_name = "asset_lat_" + str(each)
            asset_lat_names.append(lat_name)
            lat_name = latitude_series.tolist()

            long_name = "asset_long_" + str(each)
            asset_long_names.append(long_name)
            long_name = longitude_series.tolist()

            asset_gps_locations[each] = (lat_name, long_name)

            lat.extend(lat_name)
            long.extend(long_name)
            
        # extending the upper time bound by an amount and finding the gps coordinates 
        # of the unique assets in this time period 
        extended_bound_time_series = pd.date_range(upper_bound_time, periods=2, freq=extended_time)
        extended_bound_time = extended_bound_time_series.tolist()[1] 

        for i in range(0, asset_count):

            unique_id = asset_id[i]
            value = asset_gps_locations.get(str(unique_id))

            asset_track_df = track_df[track_df['packet_generating_station_id'] == asset_id[i]]
            lower_bound_asset_track_df = asset_track_df[asset_track_df['device_time_stamp'] > upper_bound_time]
            bounded_asset_track_df = lower_bound_asset_track_df[lower_bound_asset_track_df['device_time_stamp'] < extended_bound_time]

            extended_asset_lat_series = bounded_asset_track_df.latitude_gps
            extended_lat_name = "extended_asset_lat" + str(i)
            extended_lat_name = extended_asset_lat_series.tolist()
            # print(len(extended_lat_name))
            lat_value = value[0]
            # print(len(lat_value))
            lat_value.extend(extended_lat_name)
            # print(len(lat_value))

            extended_asset_long_series = bounded_asset_track_df.longitude_gps
            extended_long_name = "extended_long_name" + str(i)
            extended_long_name = extended_asset_long_series.tolist()
            # print(len(extended_long_name))
            long_value = value[1]
            # print(len(long_value))
            long_value.extend(extended_long_name)
            # print(len(long_value))
            
        # plotting the path of each individual asset on google maps
        for each in asset_gps_locations.keys():
            map_name = str(each) + "_Analysis_Map.html"
            
            # initializing the gmap plots
            gmap = gmplot.GoogleMapPlotter(plant_latitude,plant_longitude, 16)
            mapping_latitudes = asset_gps_locations.get(each)[0]
            mapping_longitudes = asset_gps_locations.get(each)[1]

            # marking the start and end gps coordinates of the asset
            # red pin is the start location
            # yellow pin is the end location
            gmap.marker(lat=mapping_latitudes[0], lng=mapping_longitudes[0], color='#FF0000')
            gmap.marker(lat=mapping_latitudes[len(mapping_latitudes) - 1], lng=mapping_longitudes[len(mapping_longitudes) -1], color='#FFFF00')

            # marker for the plant location
            gmap.marker(lat=plant_latitude, lng=plant_longitude, color='#000000')

            # fence around the plant
            gmap.circle(lat=plant_latitude, lng=plant_longitude, color='#000000', radius=100)

            for i in range(len(mapping_latitudes)):
                gmap.plot(mapping_latitudes, mapping_longitudes, '#87CEFA', edge_width=10)

            gmap.draw(map_name)



if __name__ == '__main__':
    
    connection = # establish the connection to the database here
    
    analysis_instance = Analyzer(connection)
    analysis_instance.runAnalysis()
    
    connection.close()

