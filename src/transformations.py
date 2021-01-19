
import logging
import json
from datetime import time, timedelta
from contextlib import suppress


import numpy as np

logger = logging.getLogger(__name__)

def print_row_on_error(func):
    def wrap(row):
        try:
            return func(row)
        except:
            logger.error(f'error proces row: {row}')
            raise
    return wrap

@print_row_on_error
def select_columns(row):
    return {
        'business_id': row['business_id'],
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

def is_open_past_aux(row, day_name, close_hour):
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

def businesses_is_open_past(close_hour):
    #use a wapper to be able to declate the hour later
    def wrapper(row):        
        return is_open_past_aux(row, 'monday',close_hour)\
            or is_open_past_aux(row, 'tuesday', close_hour)\
            or is_open_past_aux(row, 'wednesday', close_hour)\
            or is_open_past_aux(row, 'thursday', close_hour)\
            or is_open_past_aux(row, 'friday', close_hour)\
            or is_open_past_aux(row, 'saturday', close_hour)\
            or is_open_past_aux(row, 'sunday', close_hour)
    return wrapper

def calculate_open_time_day_aux(open_hour, close_hour):
    if (close_hour is  None) or (open_hour is None):
        return timedelta(minutes=0)

    #we need a timedelta to make opetations
    open_time = timedelta(hours=open_hour.hour,
                            minutes=open_hour.minute)

    close_time = timedelta(hours=24)\
        if close_hour == time(0,0)\
        else timedelta(hours=close_hour.hour,minutes=close_hour.minute)
        
    if (close_hour < open_hour) and close_hour != time(0,0):
        #if the business close the next day, hours like 10:00-2:00
        return close_time + (timedelta(hours=24) - open_time)
    else:
        return close_time - open_time

def calculate_open_time_day(row, day_name):
    return calculate_open_time_day_aux(row[f'open_hour_{day_name}'], row[f'close_hour_{day_name}'])

def calculate_open_and_close_time(row):
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
    def wrapper(values):
        return int(np.percentile(list(values) ,percentile))
    return wrapper

@print_row_on_error
def gen_triplet_key (row):
    return (row['state'] + '/' + row['city'] + '/' + row['postal_code'])

def deconstruc_tiplet_key(key):
    return key.split('/') # tuple with (state, city, postal_code)
    

def join_percentiles_remove_dict(row):
    state, city, postal_code  = deconstruc_tiplet_key(row[0])
    return{
            'state': state,
            'city': city,
            'postal_code': postal_code,
            'open_p50': row[1]['open_p50'][0],
            'open_p95': row[1]['open_p95'][0],
            'close_p50': row[1]['close_p50'][0],
            'close_p95': row[1]['close_p95'][0]
        }

def decode_json(row):
    with suppress(ValueError), suppress(json.decoder.JSONDecodeError):
        json_object = json.loads(row)
        return json_object
    logger.warning(f'can not decode row: {row}')
    return None

def join_reviews_remove_dict(row):
    num_cool_reviews = row[1]['cool_reviews'][0] if len(row[1]['cool_reviews'])>0 else 0
    if len(row[1]['businesses'])>0:
        #if after do the join we have reviews but no business
        return dict(row[1]['businesses'][0], cool_reviews=num_cool_reviews)
    else:
        None

def select_business_most_cool_reviews(businesses):
    business_most_cool_reviews = {'cool_reviews': -1} #negative to set a real value at the firt iteration of the loop
    for buisness in businesses:
        if buisness['cool_reviews'] > business_most_cool_reviews['cool_reviews']:
            business_most_cool_reviews = buisness
    return business_most_cool_reviews

def format_to_write_business_count(delimiter):
    @print_row_on_error
    def wrap(row):
        state, city = row[0].split('/')
        count = row[1]
        return delimiter.join((state, city, str(count)))
    return wrap

def format_output(columns, delimiter):
    """convert a dict to a csv row """
    def wrap(row):
        output = []
        for colum in columns:
            output.append(str(row[colum]))
        return delimiter.join(output)
    return wrap
