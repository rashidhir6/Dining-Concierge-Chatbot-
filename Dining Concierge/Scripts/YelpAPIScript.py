import yelpapi
from yelpapi import YelpAPI
import json
import boto3
from decimal import Decimal
import requests
import datetime

dynamodb = boto3.resource('dynamodb',region_name='us-east-1')
table = dynamodb.Table("yelp-restaurants")

region = 'us-east-1'
headers = {"Content-Type": "application/json"}

# write private api_key to access yelp here
api_key = 'knlva8oSDmjQysRNaUOaQXm-qMp-D5FQVXYo2K8yHEZmSCMBKn2sSaR1SSdqi6c53k6f_vuHNz06i7sb8Ez1a9T-_InebvTnckbV_9-hYx18K3Eim1bOhvnGwZZBYHYx'

yelp_api = YelpAPI(api_key)

data = ['id', 'name', 'review_count', 'rating', 'coordinates', 'address1', 'zip_code', 'display_phone']
es_data = ['id']

#cuisines = ["thai"]
#cuisines = ["asian"]
# cuisines = ["indian"]
# cuisines = ["italian"]
# cuisines = ["american"]
# cuisines = ["mexican"]


def fill_database(response, cuisine):
    new_response = json.loads(json.dumps(response), parse_float=Decimal)
    counter = 0
    for business in new_response["businesses"]:
        keyCheck  = table.get_item(Key={'restaurantID':business['id']})
        if 'Item' in keyCheck:
            continue

        try:
            details = {
                "insertedAtTimestamp": str(datetime.datetime.now()),
                "restaurantID": business["id"],
                "alias": business["alias"],
                "name": business["name"],
                "rating": Decimal(business['rating']),
                "numReviews": int(business["review_count"]),
                "address": business["location"]["display_address"],
                "latitude": str(business["coordinates"]["latitude"]),
                "longitude": str(business["coordinates"]["longitude"]),
                "zip_code": business['location']['zip_code'],
                "cuisine": cuisine,
                "city:": business['location']['city']
            }

            table.put_item(Item=details)
            counter = counter + 1

        except Exception as e:
            print("Error", e)
            exit(1)
    print(counter)

def get_data(event=None, context=None):
    for cuisine in cuisines:
        for x in range(0, 1000, 50):
            response = yelp_api.search_query(term=cuisine, location='manhattan', limit=50, offset=x)
            # print(response)
            fill_database(response, cuisine)

get_data()