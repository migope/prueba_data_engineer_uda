from __future__ import absolute_import

import argparse
import logging
import json
from datetime import time

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from  apache_beam.transforms.deduplicate import Deduplicate
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

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
        else: 
            return row[f'close_hour_{day_name}'] > close_hour

    #use a wapper to be able to declate the hour later
    def wapper(row):        
        return is_open_past_aux(row, 'monday')\
            or is_open_past_aux(row, 'tuesday')\
            or is_open_past_aux(row, 'wednesday')\
            or is_open_past_aux(row, 'thursday')\
            or is_open_past_aux(row, 'friday')\
            or is_open_past_aux(row, 'saturday')\
            or is_open_past_aux(row, 'sunday')
    return wapper


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
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

        )
        #The number of businesses that are open past 21:00, by city and state pair.
        ( businesses | 'businesses open past 21:00' >> beam.Filter(businesses_is_open_past(time(21,00)))
            | 'pair_by_key' >> beam.Map(lambda x: ((x['state'] + '/' + x['city']), 1))
            | 'count_elements' >> beam.CombinePerKey(sum)
        ) | 'print' >> beam.Map(print)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

