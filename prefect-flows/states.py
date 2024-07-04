import requests
import pandas as pd
from dotenv import load_dotenv
import os
load_dotenv()

def states_data():
    url = os.getenv('api_url')

    headers = {
        "x-rapidapi-key": os.getenv('api_key'),
        "x-rapidapi-host": os.getenv('api_host')
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        states = response.json()
        get_states = []
        id_count = 1
        for state in states:
            state_dict = {
                'state_id': id_count,
                'state_name': state['name'],
                'state_abbrv': state['postal'],
                'capital': state['capital']['name'],
                'latitude': state['capital']['latitude'],
                'longitude': state['capital']['longitude'],
            }
            get_states.append(state_dict)
            id_count += 1
        states_df = pd.DataFrame(get_states)
    return states_df

def main():
    states_df = states_data()
    df = pd.to_csv()
    filename = 'states.csv'
    filepath = os.getenv('destination')+filename
    df.to_csv(filepath,index=False)