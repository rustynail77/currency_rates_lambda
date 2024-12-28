import json
import boto3
import os
import requests
from datetime import datetime
import pytz
from decimal import Decimal

# Initialize AWS services
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')
table = dynamodb.Table('BasicSettings')

# Constants
API_KEY = os.environ['EXCHANGE_RATES_API_KEY']
BASE_URL = os.environ['EXCHANGE_RATES_API_BASE_URL']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
CURRENCIES = ['HUF', 'ILS', 'EUR', 'USD']

def fetch_eur_rates():
    url = f"{BASE_URL}/latest"
    params = {
        'access_key': API_KEY,
        'symbols': ','.join(CURRENCIES)
    }
    
    response = requests.get(url, params=params)
    if not response.ok:
        raise Exception(f"API call failed: {response.text}")
    
    data = response.json()
    if not data.get('success'):
        raise Exception(f"API returned error: {json.dumps(data)}")
    
    return data['rates']

def calculate_all_rates(eur_rates):
    result = {}
    
    # Calculate rates for each base currency
    for base in CURRENCIES:
        base_rate = Decimal(str(eur_rates[base])) if base != 'EUR' else Decimal('1')
        
        # Calculate rates for this base currency
        rates = {}
        for target in CURRENCIES:
            if target == base:
                rates[target] = Decimal('1')
            else:
                target_rate = Decimal(str(eur_rates[target])) if target != 'EUR' else Decimal('1')
                rates[target] = Decimal(str(target_rate / base_rate))
        
        # Store in the format matching Node.js structure
        result[base] = {
            'date': datetime.now(pytz.timezone('Asia/Jerusalem')).strftime('%Y-%m-%d'),
            'success': True,
            'timestamp': int(datetime.now().timestamp()),
            'base': base,
            'rates': rates
        }
    
    return result

def send_notification(message):
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject='Exchange Rates Lambda Alert',
        Message=message
    )

def lambda_handler(event, context):
    try:
        # Fetch EUR rates (only available in free tier)
        eur_rates = fetch_eur_rates()
        
        # Calculate all cross-rates
        new_rates = calculate_all_rates(eur_rates)
        
        # Update DynamoDB
        table.update_item(
            Key={'key': 'dailyExchangeRates'},
            UpdateExpression='SET #val = :val, updatedAt = :updatedAt, updatedBy = :updatedBy',
            ExpressionAttributeNames={'#val': 'value'},
            ExpressionAttributeValues={
                ':val': new_rates,
                ':updatedAt': datetime.now().isoformat(),
                ':updatedBy': 'system'
            }
        )
        
        send_notification(f"Successfully updated exchange rates: {new_rates}")
        return {
            'statusCode': 200,
            'body': 'Successfully updated exchange rates'
        }
        
    except Exception as e:
        error_msg = f"Lambda execution failed: {str(e)}"
        print(error_msg)
        send_notification(error_msg)
        return {
            'statusCode': 500,
            'body': error_msg
        } 