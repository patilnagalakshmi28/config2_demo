import json
import os
import asyncio
import boto3
from glide import (
    GlideClusterClient,
    GlideClusterClientConfiguration,
    NodeAddress,
    Logger,
    LogLevel
)

# Get VALKEY_HOST from environment variable
VALKEY_HOST = os.getenv('VALKEY_HOST')  # Fetches from Lambda environment variable
VALKEY_PORT = 6379  # Assuming default port
USE_TLS = True

# Setup DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('config_table')

Logger.set_logger_config(LogLevel.INFO)

# DynamoDB Operations
async def write_to_dynamodb(config_key, config_value):
    try:
        print(f"Writing to DynamoDB: key={config_key}, value={config_value}")
        await asyncio.to_thread(table.put_item, Item={'key': config_key, 'value': config_value})
        print(f"Successfully wrote {config_key} to DynamoDB")
    except Exception as e:
        print(f"Error writing to DynamoDB: {e}")
        raise

async def read_from_dynamodb(config_key):
    try:
        print(f"Reading from DynamoDB: key={config_key}")
        response = await asyncio.to_thread(table.get_item, Key={'key': config_key})
        print(f"Read from DynamoDB response: {response}")
        return response.get('Item')
    except Exception as e:
        print(f"Error reading from DynamoDB: {e}")
        raise

async def update_dynamodb(config_key, updated_values):
    try:
        print(f"Updating DynamoDB: key={config_key}, updated_values={updated_values}")
        update_expression = "SET "
        expression_attribute_names = {"#v": "value"}
        expression_attribute_values = {}

        for i, (k, v) in enumerate(updated_values.items()):
            update_expression += f"#v.{k} = :val{i}, "
            expression_attribute_values[f":val{i}"] = v

        update_expression = update_expression.rstrip(", ")

        await asyncio.to_thread(table.update_item,
                                 Key={'key': config_key},
                                 UpdateExpression=update_expression,
                                 ExpressionAttributeNames=expression_attribute_names,
                                 ExpressionAttributeValues=expression_attribute_values)
        print(f"Successfully updated {config_key} in DynamoDB")
    except Exception as e:
        print(f"Error updating DynamoDB: {e}")
        raise

# Glide (Valkey) Client setup
async def get_valkey_client():
    if not VALKEY_HOST:
        raise ValueError("VALKEY_HOST environment variable not set!")
    config = GlideClusterClientConfiguration(
        addresses=[NodeAddress(VALKEY_HOST, VALKEY_PORT)],
        use_tls=USE_TLS
    )
    return await GlideClusterClient.create(config)

# Lambda handler function
async def handler(event, context):
    method = event['httpMethod']
    loop = asyncio.get_event_loop()

    if method == 'POST':
        body = json.loads(event.get('body', '{}'))
        config_key = body.get('config_key')
        config_value = body.get('config_value')

        if not config_key or not config_value:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "config_key and config_value required"})
            }

        # Run both DynamoDB and Valkey writes in parallel
        client = await get_valkey_client()
        try:
            await asyncio.gather(
                loop.run_in_executor(None, write_to_dynamodb, config_key, config_value),
                client.set(config_key, json.dumps(config_value)),
                return_exceptions=True
            )
            print(f"Data for {config_key} written to Valkey and DynamoDB")
        except Exception as e:
            print(f"Error during POST operation: {e}")
            return {
                "statusCode": 500,
                "body": json.dumps({"message": f"Error occurred: {str(e)}"})
            }
        finally:
            await client.close()

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Stored in DynamoDB and Valkey"})
        }

    elif method == 'GET':
        params = event.get('queryStringParameters', {})
        config_key = params.get('key') if params else None

        if not config_key:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing query parameter 'key'"})
            }

        # Try Valkey first
        client = await get_valkey_client()
        val = await client.get(config_key)
        await client.close()

        if val:
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "config_key": config_key,
                    "config_value": json.loads(val.decode('utf-8'))
                })
            }

        # Fallback to DynamoDB
        item = await read_from_dynamodb(config_key)

        if item:
            # Cache back into Valkey
            client = await get_valkey_client()
            await client.set(config_key, json.dumps(item['value']))
            await client.close()

            return {
                "statusCode": 200,
                "body": json.dumps(item)
            }

        return {
            "statusCode": 404,
            "body": json.dumps({"message": "Key not found"})
        }

    elif method == 'PATCH':
        body = json.loads(event.get('body', '{}'))
        config_key = body.get('config_key')
        updated_values = body.get('config_value')

        if not config_key or not updated_values:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "config_key and config_value required"})
            }

        # Update DynamoDB and Valkey concurrently
        client = await get_valkey_client()
        try:
            await asyncio.gather(
                loop.run_in_executor(None, update_dynamodb, config_key, updated_values),
                client.set(config_key, json.dumps(updated_values)),
                return_exceptions=True
            )
            print(f"Data for {config_key} updated in Valkey and DynamoDB")
        except Exception as e:
            print(f"Error during PATCH operation: {e}")
            return {
                "statusCode": 500,
                "body": json.dumps({"message": f"Error occurred: {str(e)}"})
            }
        finally:
            await client.close()

        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"{config_key} updated in DynamoDB and Valkey"})
        }

    return {
        "statusCode": 405,
        "body": json.dumps({"message": "Method not allowed"})
    }

# Lambda function entry point
def lambda_handler(event, context):
    return asyncio.run(handler(event, context))
