from unittest import TestCase
from datetime import time, timedelta
from src.transformations import format_output, calculate_open_time_day_aux, decode_json
from src.transformations import join_percentiles_remove_dict, join_reviews_remove_dict, percentile

import pdb

class TransformationsTest(TestCase):
    def test_format_output(self):
        cols_name = ['col1', 'col2', 'col3']
        delimiter = ';'
        row_dict = {'col1':1, 'col2': 'foo', 'col3': True}
        
        func = format_output(cols_name, delimiter)
        result = func(row_dict)

        self.assertEquals('1;foo;True', result)
        

    def test_calculate_open_time_day_aux_1(self):
        open_hour = time(10,0)
        close_hour = time(23,0)
        expected = timedelta(hours=13)

        result = calculate_open_time_day_aux(open_hour, close_hour)

        self.assertEquals(expected, result)
    
    def test_calculate_open_time_day_aux_2(self):
        open_hour = time(10,0)
        close_hour = time(0,0) # business than close at 00:00
        expected = timedelta(hours=14)

        #pdb.set_trace()
        result = calculate_open_time_day_aux(open_hour, close_hour)

        self.assertEquals(expected, result)

    def test_calculate_open_time_day_aux_3(self):
        open_hour = time(10,0)
        close_hour = time(2,0) #business that close the next day
        expected = timedelta(hours=16)

        result = calculate_open_time_day_aux(open_hour, close_hour)

        self.assertEquals(expected, result)
    
    def test_calculate_open_time_day_aux_4(self):
        open_hour = time(0,0)
        close_hour = time(0,0)
        expected = timedelta(hours=24)

        result = calculate_open_time_day_aux(open_hour, close_hour)

        self.assertEquals(expected, result)

    def test_decode_json_correct(self):
        input_str = """{"business_id":"6OAZjbxqM5ol29BuHsil3w","name":"Nevada House of Hose","address":"1015 Sharp Cir",
            "attributes":{
            "BusinessParking":"{'garage': False, 'validated': False, 'lot': True, 'valet': False}",
            "RestaurantsPriceRange2":"4"},
            "hours":{"Monday":"7:0-16:0","Tuesday":"7:0-16:0"}}"""
        expected = {
                "business_id":"6OAZjbxqM5ol29BuHsil3w",
                "name":"Nevada House of Hose",
                "address":"1015 Sharp Cir",
                "attributes":{
                    "BusinessParking":"{'garage': False, 'validated': False, 'lot': True, 'valet': False}",
                    "RestaurantsPriceRange2":"4"},
                "hours":{
                    "Monday":"7:0-16:0",
                    "Tuesday":"7:0-16:0"}}
        result = decode_json(input_str)
        self.assertDictEqual(expected, result)

    def test_decode_json_fails(self):
        input_str = "foo"
        result = decode_json(input_str)
        self.assertIsNone(result)
    
    def test_percentile_95(self):
        func = percentile(95)
        result = func(range(1,101, 1))
        self.assertEquals(95, result)

    def test_join_percentiles_remove_dict(self):
        input_row = (
            'my_state/my_city/my_cp',
            {'open_p50': [1],
             'open_p95': [2],
             'close_p50': [3],
             'close_p95': [4]
            })

        expected = {
            'state': 'my_state',
            'city': 'my_city',
            'postal_code': 'my_cp',
            'open_p50': 1,
            'open_p95': 2,
            'close_p50': 3,
            'close_p95': 4
            }
        
        result = join_percentiles_remove_dict(input_row)
        self.assertDictEqual(expected, result)
    
    def test_join_reviews_remove_dict(self):
        input_row = (
            'my_key',
            {'businesses': [{'state': 'my_state','city': 'my_city','postal_code': 'my_cp'}],
             'cool_reviews': [5]
            })
        expected = {
            'state': 'my_state',
            'city': 'my_city',
            'postal_code': 'my_cp',
            'cool_reviews': 5}

        result = join_reviews_remove_dict(input_row)
        self.assertDictEqual(result, expected)

    def test_join_reviews_remove_dict_no_business(self):
        """has not join any business """
        input_row = (
            'my_key',
            {'businesses': [],
             'cool_reviews': [2]
            })

        result = join_reviews_remove_dict(input_row)
        self.assertIsNone(result)
    
    def test_join_reviews_remove_dict_no_reviews(self):
        """has not join any review """
        input_row = (
            'my_key',
            {'businesses': [{'state': 'my_state','city': 'my_city','postal_code': 'my_cp'}],
             'cool_reviews': []
            })
        expected = {
            'state': 'my_state',
            'city': 'my_city',
            'postal_code': 'my_cp',
            'cool_reviews': 0}

        result = join_reviews_remove_dict(input_row)
        self.assertDictEqual(result, expected)
