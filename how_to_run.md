# How To Run

This process has been developed with **Python 3.7**

## Local
1º Download and extract the data in 'data/'

2º Install the dependencies `pip install -r requirements.txt`

3º Run the process with
```bash
python3 src/main.py \
    --input_businesses data/yelp_academic_dataset_business.json \
    --input_review data/yelp_academic_dataset_review.json \
    --output_business_percentiles_prefix data/percentiles \
    --output_close_business_prefix /data/businesses_close_21 \
    --output_business_cool_reviews_prefix /data/businesses_reviews
```
## Docker
1º Build the image `docker build -t yelp_pipeline .`

2º Dowload and extract the data on your computer

3º Run the image and mount in /data the path were you have the data
```
docker run -it --rm -v $PWD/data:/data yelp_pipeline
```
* You can change the default paths of the data by arguments 
```
docker run -it --rm -v $PWD/data:/data yelp_pipeline \ 
--input_businesses /data/foo.json \
--input_review data/baar.json \
--output_business_percentiles_prefix data/percentiles \
--output_close_business_prefix /data/businesses_close_21 \
--output_business_cool_reviews_prefix /data/businesses_reviews
```


## Run Test

1º install the dependencies `pip install -r requirements.txt`

2º Run all test with `python -m unittest discover` or one test file with `python -m unittest test/<test file>.py`
