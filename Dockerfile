FROM python:3.7
COPY ./src /src
COPY ./requirements.txt requirements.txt
RUN pip install -r requirements.txt


ENTRYPOINT  [ "python", "src/main.py" ]
CMD [ "--input_businesses", "/data/yelp_academic_dataset_business.json", "--input_review", "/data/yelp_academic_dataset_review.json", "--output_business_percentiles_prefix", "/data/percentiles", "--output_close_business_prefix", "/data/businesses_close_21", "--output_business_cool_reviews_prefix", "/data/businesses_reviews" ]