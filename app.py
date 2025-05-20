import json
import boto3
import os
import asyncio
from glide import GlideClusterClientConfiguration, NodeAddress, GlideClusterClient

# DynamoDB setup
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('config_table')

# Environment variables for Valkey/Redis host and port
valkey_host = os.environ.get('VALKEY_HOST')
valkey_port = int(os.environ.get('VALKEY_PORT', 6379))

async def get_valkey_client():
    # Create config with NodeAddress list and create client asynchronously
    print(f"Connecting to Valkey at {valkey_host}:{valkey_port}")
    addresses = [NodeAddress(valkey_host, valkey_port)]
    config = GlideClusterClientConfiguration(addresses,use_tls=True)
    client = await GlideClusterClient.create(config)
    return client

def lambda_handler(event, context):
    return asyncio.run(handle_request(event))

async def handle_request(event):
    method = event['httpMethod']
    client = await get_valkey_client()

    try:
        # POST - Store config_key and config_value
        if method == 'POST':
            body = json.loads(event['body'])
            config_key = body.get('config_key')
            config_value = body.get('config_value')

            if not config_key or not config_value:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "config_key and config_value required"})
                }

            # Save to DynamoDB
            table.put_item(Item={'key': config_key, 'value': config_value})

            # Save to Redis using Valkey GLIDE
            await client.set(config_key, json.dumps(config_value))

            return {
                "statusCode": 200,
                "body": json.dumps({"message": "Stored"})
            }

        # GET - Retrieve value by config_key
        elif method == 'GET':
            params = event.get('queryStringParameters')
            if not params or 'key' not in params:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "Missing query parameter 'key'"})
                }

            key = params['key']

            # Try Redis cache first
            cached_value = await client.get(key)
            if cached_value:
                return {
                    "statusCode": 200,
                    "body": json.dumps({'key': key, 'value': json.loads(cached_value)})
                }

            # Fallback to DynamoDB
            response = table.get_item(Key={'key': key})
            item = response.get('Item')

            if item:
                # Cache the value in Redis for next time
                await client.set(key, json.dumps(item['value']))
                return {
                    "statusCode": 200,
                    "body": json.dumps(item)
                }
            else:
                return {
                    "statusCode": 404,
                    "body": json.dumps({"error": "Not found"})
                }

        # PATCH - Update value for existing config_key
        elif method == 'PATCH':
            body = json.loads(event['body'])
            config_key = body.get('config_key')
            updated_values = body.get('config_value')

            if not config_key or not updated_values:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "config_key and config_value required"})
                }

            # Update nested fields in DynamoDB item
            update_expression = "SET "
            expression_attribute_names = {"#v": "value"}
            expression_attribute_values = {}

            for i, (k, v) in enumerate(updated_values.items()):
                path = f"#v.{k}"
                value_key = f":val{i}"
                expression_attribute_values[value_key] = v
                update_expression += f"{path} = {value_key}, "

            update_expression = update_expression.rstrip(", ")

            table.update_item(
                Key={'key': config_key},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values
            )

            # Also update Redis cache
            existing_val = await client.get(config_key)
            if existing_val:
                existing_json = json.loads(existing_val)
                existing_json.update(updated_values)
                await client.set(config_key, json.dumps(existing_json))

            return {
                "statusCode": 200,
                "body": json.dumps({'message': f'Updated {config_key} successfully'})
            }

        else:
            return {
                "statusCode": 405,
                "body": json.dumps({"error": "Method Not Allowed"})
            }

    finally:
        await client.close()
