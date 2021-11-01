from concurrent.futures.thread import _WorkItem
import requests
import base64
from creds import client_creds
from concurrent.futures import ThreadPoolExecutor, as_completed
import zlib
import datetime
from requests.api import head
import os

# request access token
def get_access_token():
    token_request_uri = "https://accounts.spotify.com/api/token"
    token_request_data = {"grant_type": "client_credentials"}
    token_request_headers = {"Authorization": f"Basic {client_creds.decode()}"}
    access_token = requests.post(token_request_uri, data=token_request_data, headers=token_request_headers).json()['access_token']
    return access_token

def get_files():
    
    # types of files
    files = ['streams','sub_30_sec_streams','users','tracks']
    labels = ['2dutchbv','demuziekfabriekbv']

    #Create request components
    url = "https://provider-api.spotify.com/v1/analytics"
    access_token = get_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}

    # Loop through filetypes to get all year files
    for f in files:
        for l in labels:
            def files_func():
                access_token = get_access_token()
                headers = {"Authorization": f"Bearer {access_token}"}
                
                #Get all available years
                response = requests.get(url=f"{url}/{l}/enhanced/{f}/",headers=headers).json()

                # Get all available months for available years
                
                uri = f"/{l}/enhanced/{f}/2021"
                months = requests.get(url=f"{url}{uri}",headers=headers).json()
                print(months)
                # Get available days for available months
                for month in months:
                    uri=month["uri"]
                    days = requests.get(url=f"{url}{uri}",headers=headers).json()
                    
                    data =[]
                    file = ""

                    # If requesting streams or skips, requests available files per country.
                    if f == "streams" or f == "sub_30_sec_streams":
                        countries = []

                        # Request country files
                        def get_countries(uri):
                            try:
                                country = requests.get(url=f"{url}{uri}",headers=headers).json()
                                print(country)
                                for i in country:
                                    countries.append(i)

                            except requests.exceptions.RequestException as e:
                                return e

                        def get_countries_runner():
                            threads = []
                            with ThreadPoolExecutor(max_workers=40) as executor:
                                for day in days:
                                    uri = day["uri"]
                                    threads.append(executor.submit(get_countries,uri))

                        get_countries_runner()

                        # Get the actual file and append results to data aray
                        def get_country_files(uri):
                            try:
                                response = requests.get(url=f"{url}{uri}",headers=headers).content
                                decompressed_data = zlib.decompress(response, 16 +zlib.MAX_WBITS)
                                data.append(decompressed_data)

                            except requests.exceptions.RequestException as e:
                                return e
                        
                        def get_country_files_runner():

                            threads=[]
                            with ThreadPoolExecutor(max_workers=40) as executor:
                                for country in countries:
                                    uri = country["uri"]
                                    threads.append(executor.submit(get_country_files,uri))

                                e = uri.split("/")
                                name = e[1] + '_' +e[3] + '_' + e[4] + '_' + e[5]
                                return name

                        file = get_country_files_runner()


                        # # Write data to JSON file
                        # with open(f"{file}.json","wb") as out:
                        #     for line in data:
                        #         out.write(line)

                        #Reset countries 
                        countries = []

                        

                    else:
                        # If requesting files is users or tracks, directly request and save files
                        def get_date_files(uri):
                            try:
                                response = requests.get(url=f"{url}{uri}",headers=headers).content
                                decompressed_data = zlib.decompress(response, 16 +zlib.MAX_WBITS)
                                data.append(decompressed_data)
                            except requests.exceptions.RequestException as e:
                                return e

                        def get_date_files_runner():
                            threads = []
                            with ThreadPoolExecutor(max_workers=50) as executor:
                                for day in days:
                                    uri = day["uri"]
                                    threads.append(executor.submit(get_date_files,uri))
                                
                                e = uri.split("/")
                                name = e[1] + '_' +e[3] + '_' + e[4] + '_' + e[5]
                                return name

                        file = get_date_files_runner()

                    # Create folder or check if exists
                    folder_names = "data_2021"
                    if not os.path.exists(folder_names):
                        os.makedirs(folder_names)

                    # Write data to JSON file
                    with open(f"{folder_names}/{file}.json","wb") as out:
                        for line in data:
                            out.write(line)

            files_func()

get_files()
    