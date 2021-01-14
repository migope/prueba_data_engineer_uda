# The challenge

## Description

The purpose of this challenge is to test your skills in using a **distributed data processing** tool to load a sample dataset and write the necessary code to answer a few statistical questions.

You can use any distributed data processing tool and language you like, but please try not to be too obscure for the sake of it (:

The main goal is to run this process nightly, so it needs to run smoothly. Take into account the following viables:
- Code Clarity: make the code as simple and clear as possible.
- Code Quality: add tests, make sure there’s no bugs
- Testability: is your code easy to test?
- Data Quality: did you add specific steps to validate input or output data with pre-conditions and post-conditions?
- Data Versioning: is it possible to have different versions of different datasets produced? How would you do it?
- Observability: when it fails, do we have plenty of information about what happened?
- Performance: does it run in the minimum amount of time?
- Scalability: what if the data volume grows 10x? How would it affect the rest of the variables?
- Monitoring: how would you make sure it’s running nightly and detect outages in near realtime?

## The dataset

For this challenge you will be using the [sample dataset](https://www.yelp.com/dataset/) Yelp.com provides for. You can find documentation on the different object types in [their documentation](https://www.yelp.com/dataset/documentation/main).

## Questions to answer

Using the *business.json* dataset, and filtering out all businesses that are closed can you calculate:

- Median and p95 opening time during the week, by postal code, city, and state triplet.
- Median and p95 closing time during the week, by postal code, city, and state triplet.
- The number of businesses that are open past 21:00, by city and state pair.

By combining the *business.json* and *review.json* dataset, can you calculate:

- For each postal code, city, and state triplet, the business with the highest number of “cool” review votes that are not open on Sunday.

## The results

Depending on the tool you might decide to use, we expect the following deliverables:

- **Short description of the components** of the distributed data processing tool setup you choose.
- A bootstrap script for **deploying and running the tool as a container**.
- **A Git repo** with the source code used to calculate the answers to those questions. We need semantic and small commits along the process. Please, don’t make a single huge commit at the end.
- **Video, screenshots or any other evidence** of the pipeline running.
- The results of each question, **one file per question**. Feel free to use the output format you want and explain why you chose it.
