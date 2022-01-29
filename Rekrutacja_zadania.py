from glob import glob
import boto3
import pandas as pd
import requests


client = boto3.client(
    's3',
    aws_access_key_id='AKIARKFFRSDULBGSULHM',
    aws_secret_access_key='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',
)

df = pd.read_csv('C:/Users/kploc/Desktop/ECONOMIST_metadata.csv')

# downloading datasets from given page
for code in df['code']:
    url = f'https://data.nasdaq.com/api/v3/datasets/ECONOMIST/{code}.csv?api_key=4uiHbiJ4BFHKiUQRgJgi'
    request = requests.get(url)
    csv = open(f'Datasets/{code}.csv', 'wb')
    csv.write(request.content)
    csv.close()

# upload datasets to s3 bucket
paths = glob('Datasets/*.csv')
for path in paths:
    client.put_object(Body=open(path, 'rb'), Bucket='bigmac-dataset', Key=path.split('\\')[1])

# get max bigmac index for given date
chosen_countries = {}
for path in paths:
    data = pd.read_csv(path)
    if '2021-07-31' in data['Date'].values:
        chosen_countries[path.split('\\')[1]] = max(data[data['Date'] == '2021-07-31']['dollar_valuation'].values)

# fillter out top 5 bigmac index
sorted_dict = {k.split('_')[1].split('.')[0]: v for k, v in sorted(chosen_countries.items(), key=lambda item: item[1])}
pd.DataFrame(list(sorted_dict.items())[-5:], columns=['name', 'dollar_valuation']).to_csv('res_df.csv')
