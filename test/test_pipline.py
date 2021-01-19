import tempfile
import unittest
from csv import DictReader
import os
import sys


from apache_beam.testing.util import open_shards


from src import transformations
sys.modules['transformations'] = transformations
from src.main import run as pipeline

class BusinessETLTest(unittest.TestCase):

    # 1º row: business that is open past 21:00
    # 2º row: business don't opend on Sundays
    business_data = [
        """{"business_id":"6OAZjbxqM5ol29BuHsil3w","name":"Nevada House of Hose","address":"1015 Sharp Cir","city":"North Las Vegas","state":"NV","postal_code":"89030","latitude":36.2197281,"longitude":-115.1277255,"stars":2.5,"review_count":3,"is_open":0,"attributes":{"BusinessAcceptsCreditCards":"True","ByAppointmentOnly":"False","DogsAllowed":"True","BikeParking":"True","BusinessParking":"{'garage': False, 'street': False, 'validated': False, 'lot': True, 'valet': False}","RestaurantsPriceRange2":"4"},"categories":"Hardware Stores, Home Services, Building Supplies, Home & Garden, Shopping","hours":{"Monday":"7:0-16:0","Tuesday":"7:0-16:0","Wednesday":"7:0-16:0","Thursday":"7:0-16:0","Friday":"7:0-22:0","Saturday":"11:0-20:0","Sunday":"13:0-18:0"}}""",
        """{"business_id":"f9NumwFMBDn751xgFiRbNA","name":"The Range At Lake Norman","address":"10913 Bailey Rd","city":"Cornelius","state":"NC","postal_code":"28031","latitude":35.4627242,"longitude":-80.8526119,"stars":3.5,"review_count":36,"is_open":1,"attributes":{"BusinessAcceptsCreditCards":"True","BikeParking":"True","GoodForKids":"False","BusinessParking":"{'garage': False, 'street': False, 'validated': False, 'lot': True, 'valet': False}","ByAppointmentOnly":"False","RestaurantsPriceRange2":"3"},"categories":"Active Life, Gun\/Rifle Ranges, Guns & Ammo, Shopping","hours":{"Monday":"10:0-18:0","Tuesday":"11:0-20:0","Wednesday":"10:0-18:0","Thursday":"11:0-20:0","Friday":"11:0-20:0","Saturday":"11:0-20:0"}}"""
    ]
    # the 2º row is has cool=1 but the business open on Sundays, so the business don't have to be in the output business_reviews
    reviews_data = ["""{"review_id":"otI2VeBHsfIMyWEhjqCikg","user_id":"vCYPAeCFE7W5tdn7fVtsBw","business_id":"f9NumwFMBDn751xgFiRbNA","stars":5.0,"useful":3,"funny":1,"cool":1,"text":"fooo","date":"2012-12-24 18:43:33"}""",
                    """{"review_id":"9agfKl6S438rsod_NChs7Q","user_id":"x4tK3cV-QT8SKFaJEig4_A","business_id":"6OAZjbxqM5ol29BuHsil3w","stars":5.0,"useful":0,"funny":0,"cool":1,"text":"baar","date":"2019-10-28 00:43:33"}"""
                ]

    busines_file = 'business.json'
    reviews_file = 'reviews.json'
    out_percetiles_files = 'percentiles'
    out_business_21_files = 'business_21'
    out_business_reviews_files = 'business_reviews'

    out_percetiles_files_expected = [
        {
            'state': 'NV',
            'city': 'North Las Vegas',
            'postal_code': '89030',
            'open_p50': float(65 * 3600), #65h at week open, converted to seconds
            'open_p95': float(65 * 3600), #we only have one business for the tripeset so p50 = p95
            'close_p50': float(103 * 3600), #103h at week close, converted to seconds
            'close_p95': float(103 * 3600),
        },
        {
            'state': 'NC',
            'city': 'Cornelius',
            'postal_code': '28031',
            'open_p50': float(52 * 3600),
            'open_p95': float(52 * 3600),
            'close_p50': float(116 * 3600),
            'close_p95': float(116 * 3600)
        }
    ]

    out_business_21_data_expected = {
        'state':'NV',
        'city':'North Las Vegas',
        'count':'1'
    }
    out_business_reviews_expected = {
        'state': 'NC',
        'city': 'Cornelius',
        'postal_code': '28031',
        'business_id': 'f9NumwFMBDn751xgFiRbNA',
        'cool_reviews': '1'
    }

    @staticmethod
    def create_input_files(file_name, content):
        with open(file_name, 'w') as f:
            f.write('\n'.join(content))
    @staticmethod
    def remove_files_prefix(prefix):
        for file in os.listdir():
            if file.startswith(prefix):
                os.remove(file)

    def _check_percentiles(self):
        with open_shards(self.out_percetiles_files + '*') as csvfile:
            out_percetiles_files_data = list(DictReader(csvfile, delimiter=';'))
            self.assertEqual(2, len(out_percetiles_files_data))

            #the rows of the csv can be in diferent order that the variable out_percetiles_files_expected
            for result in out_percetiles_files_data:
                is_correct = False
                for expected in self.out_percetiles_files_expected:
                    #find the row by state, city and postal_code
                    if (result['state'] == expected['state']) and (result['city'] == expected['city']) and (result['postal_code'] == expected['postal_code']):
                        self.assertEqual(expected['open_p50'], float(result['open_p50']))
                        self.assertEqual(expected['open_p95'], float(result['open_p95']))
                        self.assertEqual(expected['close_p50'], float(result['close_p50']))
                        self.assertEqual(expected['close_p95'], float(result['close_p95']))
                        is_correct=True
                        break
                if not is_correct:
                    raise Exception(f'conbination of state, city and postal_code not expected: {result}')

    def _check_busines_opend_21(self):
        with open_shards(self.out_business_21_files + '*') as csvfile:
            out_business_21_data = list(DictReader(csvfile, delimiter=';'))
            self.assertEqual(1, len(out_business_21_data))
            self.assertDictEqual(dict(out_business_21_data[0]), self.out_business_21_data_expected)

    def _check_business_reviews(self):
        with open_shards(self.out_business_reviews_files + '*') as csvfile:
            out_business_reviews_data = list(DictReader(csvfile, delimiter=';'))
            self.assertEqual(1, len(out_business_reviews_data))
            self.assertDictEqual(self.out_business_reviews_expected, dict(out_business_reviews_data[0]))

    def test_pipeline(self):

        self.create_input_files(self.busines_file, self.business_data)
        self.create_input_files(self.reviews_file, self.reviews_data)

        pipeline(
            [
                "--input_businesses", self.busines_file, 
                "--input_review", self.reviews_file, 
                "--output_business_percentiles_prefix", self.out_percetiles_files,
                "--output_close_business_prefix", self.out_business_21_files,
                "--output_business_cool_reviews_prefix", self.out_business_reviews_files
            ],
            save_main_session=False
        )

        
        #check output of percentiles
        self._check_percentiles()

        #check output of business open past 21
        self._check_busines_opend_21()

        # check ouput of the busness with cool reviews
        self._check_business_reviews()

        #clean up
        os.remove(self.busines_file)
        os.remove(self.reviews_file)
        self.remove_files_prefix(self.out_percetiles_files)
        self.remove_files_prefix(self.out_business_21_files)
        self.remove_files_prefix(self.out_business_reviews_files)