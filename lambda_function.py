import json
import boto3


def lambda_handler(event, context):

    for i in event['Records']:
        action = i['eventName']
        bucket_name = i['s3']['bucket']['name']

    client = boto3.client('ses')
    subject = 'Dodanie danych do s3'
    body = '''
        <h1>Hello!</h1>
        <br>
        It's Krystian Plochocki and this email is to notify you about {} event, which means that all files has been added to s3.
    '''.format(action)

    message = {'Subject': {'Data': subject}, 'Body': {'Html': {'Data': body}}}
    response = client.send_email(Source='k.ploch111@gmail.com', Destination={'ToAddresses': ['monika.kulisz@onwelo.com']}, Message=message)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
