import json
import os
import asyncio
import boto3
import traceback
from glide import GlideClusterClientConfiguration, NodeAddress, GlideClusterClient

# DynamoDB setup
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('config_table')

# Environment variables for Valkey host and port
valkey_host = os.environ.get('VALKEY_HOST')
valkey_port = int(os.environ.get('VALKEY_PORT', 6379))

# Global Glide client for reuse across Lambda invocations
valkey_client = None

async def get_valkey_client():
    global valkey_client
    if valkey_client is None:
        print(f"Connecting to Valkey at {valkey_host}:{valkey_port}")
        addresses = [NodeAddress(valkey_host, valkey_port)]
        config = GlideClusterClientConfiguration(addresses=addresses, use_tls=True)
        valkey_client = await GlideClusterClient.create(config)
    return valkey_client

def lambda_handler(event, context):
    try:
        return asyncio.run(handle_request(event))
    except Exception as e:
        print("Unhandled exception in lambda_handler:", str(e))
        traceback.print_exc()
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

async def handle_request(event):
    method = event['httpMethod']
    client = await get_valkey_client()

    try:
        if method == 'POST':
            try:
                body = json.loads(event.get('body') or '{}')
            except json.JSONDecodeError:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "Invalid JSON body"})
                }

            config_key = body.get('config_key')
            config_value = body.get('config_value')

            if not config_key or config_value is None:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "config_key and config_value required"})
                }

            # Save to DynamoDB
            table.put_item(Item={'key': config_key, 'value': config_value})

            # Save to Valkey
            await client.set(config_key, json.dumps(config_value))

            return {
                "statusCode": 200,
                "body": json.dumps({"message": "Stored"})
            }

        elif method == 'GET':
            params = event.get('queryStringParameters')
            if not params or 'key' not in params:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "Missing query parameter 'key'"})
                }

            key = params['key']

            # Try Redis (Valkey) cache first
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

        elif method == 'PATCH':
            try:
                body = json.loads(event.get('body') or '{}')
            except json.JSONDecodeError:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "Invalid JSON body"})
                }

            config_key = body.get('config_key')
            updated_values = body.get('config_value')

            if not config_key or not isinstance(updated_values, dict):
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "config_key and config_value required"})
                }

            # Construct update expression
            update_expression = "SET "
            expression_attribute_names = {"#v": "value"}
            expression_attribute_values = {}
            for i, (k, v) in enumerate(updated_values.items()):
                path = f"#v.{k}"
                value_key = f":val{i}"
                expression_attribute_values[value_key] = v
                update_expression += f"{path} = {value_key}, "
            update_expression = update_expression.rstrip(", ")

            # Update DynamoDB
            table.update_item(
                Key={'key': config_key},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values
            )

            # Update Redis cache if it exists
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

    except Exception as e:
        print("Error during request handling:", str(e))
        traceback.print_exc()
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
