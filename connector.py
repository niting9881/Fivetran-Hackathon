import requests as rq
import traceback
import datetime
import json

#Step 1: Define the required imports
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log 
from fivetran_connector_sdk import Operations as op

def schema(configuration: dict):
	#Step 2: Define the schema for two tables:
	# - top_headlines
	# - sources
	
	top_headlines_table = {
		"table": "top_headlines",
		"primary_key": ["url"],
		"columns": {
			"source_id": "STRING",
			"source_name": "STRING",
			"published_at": "UTC_DATETIME",
			"author": "STRING",
			"title": "STRING",
			"description": "STRING",
			"url": "STRING",
			"content": "STRING"
		}
	}

	sources_table = {
		"table": "sources",
		"primary_key": ["id"],
		"columns": {
			"id": "STRING",
			"name": "STRING",
			"description": "STRING",
			"url": "STRING",
			"category": "STRING",
			"language": "STRING",
			"country": "STRING"
		}
	}

	tables = [
		top_headlines_table,
		sources_table
	]

	return tables


def update(configuration: dict, state: dict):
	#Step 3: Define the logic for the update method

	top_headlines_url = "https://newsapi.org/v2/top-headlines" #Step 3.1 define the top_headlines_url
	sources_url = "https://newsapi.org/v2/top-headlines/sources" #Step 3.2 define the sources_url
	
	try:
		#There are 6 parameters that we need to define for top_headlines
		# 1. from
		# 2. to
		# 3. page
		# 4. language
		# 5. sortBy
		# 6. pageSize

		#There is 1 parameter that we need to define for sources
		# 1. language

		if 'to_ts' in state:
			from_ts = state['to_ts'] #Step 3.3 if a state exists, pull that state
		else:
			from_ts = datetime.datetime.now() - datetime.timedelta(days=7) #Step 3.4 if there is no state, define the date from which to start the lookback
			from_ts = from_ts.strftime("%Y-%m-%dT%H:%M:%S")

		to_ts = datetime.datetime.now() #Step 3.5 define the end of the time range from which to stop the search
		to_ts = to_ts.strftime("%Y-%m-%dT%H:%M:%S")
		headers = {
			"Authorization": "Bearer {}".format(
				configuration["API_KEY"] #Step 3.6 define the API by pulling it from the configuration.json
			),
			"accept": "application/json"
		}

		top_headlines_params = {
			#Step 3.6 supply the defined parameters for top_headlines, note that two of these come
			#from the configuration.json
			"from": from_ts,
			"to": to_ts,
			"page": 1,
			"language": configuration['language'],
			"sortBy": "publishedAt",
			"pageSize": configuration['pageSize']
		}


		yield from sync_top_headlines(
			top_headlines_url,
			headers,
			top_headlines_params,
			state
		)

		sources_params = {
			#Step 3.7 supply the parameters for sources, note that this parameter comes from
			#the configuration.json
			"language": configuration['language']
		}

		yield from sync_sources(
			sources_url,
			headers,
			sources_params
		)

		new_state = { #Step 3.8 define the new_state parameter
			"to_ts": to_ts
		}

		log.fine(
			f"state updated, new state: {repr(new_state)}"
		)

		#Step 3.9 checkpoint the new state
		yield op.checkpoint(
			state=new_state
		)

	except Exception as e:
		exception_message = str(e)
		stack_trace = traceback.format_exc()
		detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
		raise RuntimeError(detailed_message)


### helper methods ###
def sync_top_headlines(base_url, headers, params, state):

	#defined by the user
	#include the while loop for pagination, set a lower page size
	#include the has more logic
	has_more_pages = True

	while has_more_pages:
		response_page = get_api_response(base_url, headers, params) #Step 4: Write the logic to get the API response via the get_api_response helper function
		
		#Step 4.1 iterate over the responses and for each response, yield a dictionary of the response values
		#mapped to your defined schema
		
		log.info(str(response_page["totalResults"]) + " results")

		items = response_page.get("articles", [])

		if not items:
			break

		for item in items:

			yield op.upsert(
				table="top_headlines",
				data={
					"source_id": item["source"]["id"],
					"source_name": item["source"]["name"],
					"published_at": item["publishedAt"],
	                "author": item["author"],
	                "title": item["title"],
	                "description": item["description"],
	                "url": item["url"],
	                "content": item["content"]
				}
			)

		#Step 4.2: checkpoint the state
		yield op.checkpoint(
			state
		)

		has_more_pages, params = pagination(
			params, response_page
		)

def sync_sources(base_url, headers, params):

	response_page = get_api_response(base_url, headers, params) #Step 5: Write the logic to get the API response via the get_api_response helper function
		
	#Step 5.1 iterate over the responses and for each response, yield a dictionary of the response values
	#mapped to your defined schema

	items = response_page.get("sources", [])

	for item in items:
		yield op.upsert(
			table="sources",
			data={
				"id": item["id"],
				"name": item["name"],
                "description": item["description"],
                "url": item["url"],
                "category": item["category"],
                "language": item["language"],
                "country": item["country"]
			}
		)


def get_api_response(endpoint_path, headers, params):

	response = rq.get(
		endpoint_path,
		headers=headers,
		params=params
	)

	response.raise_for_status()
	response_page = response.json()

	return response_page


def pagination(params, response_page):

	has_more_pages = True
	
	current_page = int(
		params["page"]
	)

	total_pages = divmod(
		int(
			response_page["totalResults"]
		),
		int(
			params["pageSize"]
		)

	)[0] + 1

	increment_page_number = current_page and total_pages and current_page < total_pages and current_page * int(params["pageSize"]) < 100

	if increment_page_number:
		params["page"] = current_page + 1
	else:
		has_more_pages = False

	return has_more_pages, params

connector = Connector( #Step 6: Create the Connector Object
	update=update,
	schema=schema
)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)