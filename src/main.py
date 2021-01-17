from __future__ import absolute_import

import argparse
import logging
import json
from datetime import time, timedelta

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from  apache_beam.transforms.deduplicate import Deduplicate
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

import numpy as np

def select_columns(row):
    return {
        'city': row['city'],
        'state': row['state'],
        'postal_code': row['postal_code'],
        'hours_monday': row['hours'].get('Monday', None),
        'hours_tuesday': row['hours'].get('Tuesday', None),
        'hours_wednesday': row['hours'].get('Wednesday', None),
        'hours_thursday': row['hours'].get('Thursday', None),
        'hours_friday': row['hours'].get('Friday', None),
        'hours_saturday': row['hours'].get('Saturday', None),
        'hours_sunday': row['hours'].get('Sunday', None),
    }

def cast_hours(raw_hour):
    hour_part, minute_part = raw_hour.split(':')
    parsed_hour = time(int(hour_part), int(minute_part))
    return parsed_hour

def parse_hours(row):
    def split_hour(day_name):
        try:
            open_hour, close_hour = row[f'hours_{day_name}'].split('-')
            #del row[f'hours_{day_name}']
            row[f'open_hour_{day_name}'] = cast_hours(open_hour)
            row[f'close_hour_{day_name}'] = cast_hours(close_hour)
        except:
            row[f'open_hour_{day_name}'] = None
            row[f'close_hour_{day_name}'] = None
    
    split_hour('monday')
    split_hour('tuesday')
    split_hour('wednesday')
    split_hour('thursday')
    split_hour('friday')
    split_hour('saturday')
    split_hour('sunday')
    return row

def print_return(row):
    print(row)
    return row

def businesses_is_open_past(close_hour):
    def is_open_past_aux(row, day_name):
        if (row[f'close_hour_{day_name}'] is  None) or (row[f'open_hour_{day_name}'] is None):
            return False
        elif row[f'close_hour_{day_name}'] < row[f'open_hour_{day_name}']:
            #if the business close the next day
            return True
        elif row[f'close_hour_{day_name}'] == time(0,0):
            #there are some bussiness that close at the change of day
            return True
        else: 
            return row[f'close_hour_{day_name}'] > close_hour

    #use a wapper to be able to declate the hour later
    def wrap(row):        
        return is_open_past_aux(row, 'monday')\
            or is_open_past_aux(row, 'tuesday')\
            or is_open_past_aux(row, 'wednesday')\
            or is_open_past_aux(row, 'thursday')\
            or is_open_past_aux(row, 'friday')\
            or is_open_past_aux(row, 'saturday')\
            or is_open_past_aux(row, 'sunday')
    return wrap

def calculate_open_and_close_time(row):
    def calculate_open_time_day_aux(open_hour, close_hour):
        if (close_hour is  None) or (open_hour is None):
            return timedelta(minutes=0)

        #we need a timedelta to make opetations
        open_time = timedelta(hours=open_hour.hour,
                              minutes=open_hour.minute)

        close_time = timedelta(hours=24)\
            if close_hour == time(0,0)\
            else timedelta(hours=close_hour.hour,minutes=close_hour.minute)
            
        if close_hour < open_hour:
            #if the business close the next day, hours like 10:00-2:00
            return close_time + (timedelta(hours=24) - open_time)
        else:
            return close_time - open_time

    def calculate_open_time_day(row, day_name):
        return calculate_open_time_day_aux(row[f'open_hour_{day_name}'], row[f'close_hour_{day_name}'])
    
    open_time_week =  calculate_open_time_day(row, 'monday')\
            + calculate_open_time_day(row, 'tuesday')\
            + calculate_open_time_day(row, 'wednesday')\
            + calculate_open_time_day(row, 'thursday')\
            + calculate_open_time_day(row, 'friday')\
            + calculate_open_time_day(row, 'saturday')\
            + calculate_open_time_day(row, 'sunday')
    row['open_time_week'] = open_time_week
    row['close_time_week'] = timedelta(days=7) - open_time_week
    return row

def percentile(percentile):
    def wrap(values):
        return np.percentile(list(values) ,percentile)
    return wrap

def gen_triplet_key(row):
    return (row['state'] + '/' + row['city'] + '/' + row['postal_code'])

def join_percentiles_remove_dict(row):
    return (
        row[0],
        {
            'open_p50': row[1]['open_p50'][0],
            'open_p95': row[1]['open_p95'][0],
            'close_p50': row[1]['close_p50'][0],
            'close_p95': row[1]['close_p95'][0]
        })

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        help='Input file to process.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    
    with beam.Pipeline(options=pipeline_options) as p:

        businesses = ( p | 'Read' >> ReadFromText(known_args.input)
            | 'parse_json' >> beam.Map(json.loads)
            | 'filter_no_hour' >> beam.Filter(lambda row: 'hours' in row) # remove the business that don't have an hour
            | 'filter_null_hour' >> beam.Filter(lambda row: row['hours'] is not None) # remove the business that have null hours
            | 'select_columns' >> beam.Map(select_columns) # select only the fields that we need
            #| 'print_return' >> beam.Map(print_return)
            | 'parse_hours' >> beam.Map(parse_hours)
            | 'calculate_open_and_close_time' >> beam.Map(calculate_open_and_close_time)

        )
        ## Median and p95 opening time during the week, by postal code, city, and state triplet.
        # Pair by city, state and postal code, for the value we only need the open time
        businesses_open_time = (
            businesses  
            | 'pair_by_city_state_postal_code_open' >> beam.Map(lambda x: (gen_triplet_key(x), x['open_time_week'].total_seconds()))
        )
        businesses_open_time_p95 = businesses_open_time  | 'calculate_open_percentile_95' >> beam.CombinePerKey(percentile(95))
        businesses_open_time_p50 = businesses_open_time  | 'calculate_open_percentile_50' >> beam.CombinePerKey(percentile(50))

        ## Median and p95 closing time during the week, by postal code, city, and state triplet.
        # Pair by city, state and postal code, for the value we only need the close time
        businesses_close_time = (
            businesses  
            | 'pair_by_city_state_postal_code_close' >> beam.Map(lambda x: (gen_triplet_key(x), x['close_time_week'].total_seconds()))
        )
        businesses_close_time_p95 = businesses_close_time  | 'calculate_close_percentile_95' >> beam.CombinePerKey(percentile(95))
        businesses_close_time_p50 = businesses_close_time  | 'calculate_close_percentile_50' >> beam.CombinePerKey(percentile(50))

        #join all percentile operations
        (
            {'open_p50': businesses_open_time_p50,
            'open_p95': businesses_open_time_p95,
            'close_p50': businesses_close_time_p50,
            'close_p95': businesses_close_time_p95} 
                | beam.CoGroupByKey()
                | beam.Map(join_percentiles_remove_dict)
                | beam.Map(print)
        )
        ## The number of businesses that are open past 21:00, by city and state pair.
        ( businesses | 'businesses open past 21:00' >> beam.Filter(businesses_is_open_past(time(21,00)))
            | 'pair_by_city_and_state' >> beam.Map(lambda x: ((x['state'] + '/' + x['city']), 1))
            | 'count_elements' >> beam.CombinePerKey(sum)
            | 'print' >> beam.Map(print)
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

