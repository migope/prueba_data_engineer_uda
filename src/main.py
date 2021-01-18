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

from transformations import select_columns, cast_hours, parse_hours, print_return, businesses_is_open_past, calculate_open_and_close_time, percentile, gen_triplet_key, join_percentiles_remove_dict, decode_json, join_reviews_remove_dict, select_business_most_cool_reviews

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_businesses',
        help='Input file to process.')
    parser.add_argument(
        '--input_review',
        help='Input file to process.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    
    with beam.Pipeline(options=pipeline_options) as p:

        businesses = ( p | 'Read' >> ReadFromText(known_args.input_businesses)
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
                | 'join_percentiles' >> beam.CoGroupByKey()
                | beam.Map(join_percentiles_remove_dict)
                | beam.Map(print)
        )
        ## The number of businesses that are open past 21:00, by city and state pair.
        ( businesses | 'businesses open past 21:00' >> beam.Filter(businesses_is_open_past(time(21,00)))
            | 'pair_by_city_and_state' >> beam.Map(lambda x: ((x['state'] + '/' + x['city']), 1))
            | 'count_businesses' >> beam.CombinePerKey(sum)
            | 'print1' >> beam.Map(print)
        )


        ## For each postal code, city, and state triplet, the business with the highest number of “cool” review votes that are not open on Sunday.
        businesses_no_sundays = (
            businesses 
                | 'business_not_opend_sundays' >> beam.Filter(lambda row: row['hours_sunday'] is not None)
                | 'business_business_id' >> beam.Map(lambda x: (x['business_id'], x) )
        )
        reviews = ( p | 'Read_reviews' >> ReadFromText(known_args.input_review)
            | 'parse_review_json' >> beam.Map(decode_json)
            | 'filter_wrong_lines' >> beam.Filter(lambda x: x is not None )
            | 'filter_cool_reviews' >> beam.Filter(lambda x: x['cool'] >0 ) # select "cool" reviews
            | 'review_business_id' >> beam.Map(lambda x: (x['business_id'], 1) )
            | 'count_reviews' >> beam.CombinePerKey(sum) # group by business_id and count number of reviews
        )
        # join businesses and reviews
        ({
            'businesses': businesses_no_sundays,
            'cool_reviews': reviews
        } | beam.CoGroupByKey()
          | 'join_reviews_remove_dict' >> beam.Map(join_reviews_remove_dict)
          | 'gen_key_city_state_postal_code_reviews' >> beam.Map(lambda x: (gen_triplet_key(x), x)) 
          | 'select_business_most_cool_reviews' >> beam.CombinePerKey(select_business_most_cool_reviews)
          | 'print2' >> beam.Map(print)
        )
        
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

