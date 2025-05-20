import json
import boto3
import asyncio
from glide import (
    GlideClusterClient,
    GlideClusterClientConfiguration,
    NodeAddress,
    Logger,
    LogLevel,
    TimeoutError,
    RequestError,
    ConnectionError,
    ClosingError
)

# DynamoDB Setup
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('config_table')

# Valkey Configuration
VALKEY_HOST = 'your-valkey-host'  # Replace with your Valkey host
VALKEY_PORT = 6379  # Typically Redis uses 6379
USE_TLS = True

Logger.set_logger_config(LogLevel.INFO)

# Valkey Helper Functions

async def set_valkey(config_key, config_value_str):
    config = GlideClusterClientConfiguration(
        addresses=[NodeAddress(VALKEY_HOST, VALKEY_PORT)],
        use_tls=USE_TLS
    )
    client = None
    try:
        client = await GlideClusterClient.create(config)
        await client.set(config_key, config_value_str)
        return True
    except Exception as e:
        print(f"Valkey set error: {e}")
        return False
    finally:
        if client:
            try:
                await client.close()
            except Exception:
                pass

async def get_valkey(config_key):
    config = GlideClusterClientConfiguration(
        addresses=[NodeAddress(VALKEY_HOST, VALKEY_PORT)],
        use_tls=USE_TLS
    )
    client = None
    try:
        client = await GlideClusterClient.create(config)
        val = await client.get(config_key)
        if val:
            return val.decode("utf-8") if isinstance(val, bytes) else str(val)
        return None
    except Exception as e:
        print(f"Valkey get error: {e}")
        return None
    finally:
        if client:
            try:
                await client.close()
            except Exception:
                pass

# Lambda Handler Function

def lambda_handler(event, context):
    method = event['httpMethod']

    # POST - Store data in DynamoDB and Valkey
    if method == 'POST':
        return handle_store(event)

    # GET - Retrieve value by config_key
    elif method == 'GET':
        return handle_get(event)

    # PATCH - Update value for existing config_key
    elif method == 'PATCH':
        return handle_update(event)

    # Unsupported method
    else:
        return {
            "statusCode": 405,
            "body": json.dumps({"error": "Method Not Allowed"})
        }

# POST - Store data in DynamoDB and Valkey
def handle_store(event):
    body = json.loads(event.get('body', '{}'))
    config_key = body.get('config_key')
    config_value = body.get('config_value')

    if not config_key or not config_value:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "config_key and config_value required"})
        }

    # Store in DynamoDB
    table.put_item(Item={
        'key': config_key,
        'value': config_value
    })

    # Store in Valkey (as cache)
    config_value_str = json.dumps(config_value)  # Convert to string for Valkey storage
    
    # Run Valkey operation concurrently with DynamoDB
    loop = asyncio.get_event_loop()
    success_valkey = loop.run_until_complete(set_valkey(config_key, config_value_str))

    if success_valkey:
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Stored in DynamoDB and Valkey"})
        }
    else:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Failed to store in Valkey"})
        }

# GET - Retrieve data from Valkey (Cache), if not found in Valkey, fall back to DynamoDB
def handle_get(event):
    params = event.get('queryStringParameters', {})
    if not params or 'key' not in params:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Missing query parameter 'key'"})
        }

    key = params['key']

    # Try Valkey first (Cache)
    loop = asyncio.get_event_loop()
    val = loop.run_until_complete(get_valkey(key))
    if val:
        try:
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "config_key": key,
                    "config_value": json.loads(val)  # Assuming val is a JSON string
                })
            }
        except Exception:
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "config_key": key,
                    "config_value": val
                })
            }

    # Fallback to DynamoDB
    response = table.get_item(Key={'key': key})
    item = response.get('Item')

    if item:
        # Cache it in Valkey for future requests
        config_value_str = json.dumps(item['value'])
        loop.run_until_complete(set_valkey(key, config_value_str))
        
        return {
            "statusCode": 200,
            "body": json.dumps(item)
        }
    else:
        return {
            "statusCode": 404,
            "body": json.dumps({"error": "Not found"})
        }

# PATCH - Update data in DynamoDB and Valkey
def handle_update(event):
    body = json.loads(event.get('body', '{}'))
    config_key = body.get('config_key')
    updated_values = body.get('config_value')

    if not config_key or not updated_values:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "config_key and config_value required"})
        }

    # Update DynamoDB
    update_expression = "SET "
    expression_attribute_names = {}
    expression_attribute_values = {}

    for i, (k, v) in enumerate(updated_values.items()):
        path = f"#v.{k}"
        value_key = f":val{i}"
        expression_attribute_names[f"#v"] = "value"  # Top-level map
        expression_attribute_values[value_key] = v
        update_expression += f"{path} = {value_key}, "

    update_expression = update_expression.rstrip(", ")

    table.update_item(
        Key={'key': config_key},
        UpdateExpression=update_expression,
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values
    )

    # Update Valkey
    config_value_str = json.dumps(updated_values)  # Convert to string for Valkey storage
    loop = asyncio.get_event_loop()
    success_valkey = loop.run_until_complete(set_valkey(config_key, config_value_str))

    if success_valkey:
        return {
            "statusCode": 200,
            "body": json.dumps({'message': f'Updated {config_key} successfully in DynamoDB and Valkey'})
        }
    else:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Failed to update in Valkey"})
        }
