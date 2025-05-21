import json
import boto3
import asyncio
import logging
import os
import socket
from typing import Optional, Dict, Any

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

# ------------------- Configuration ------------------- #
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
Logger.set_logger_config(LogLevel.INFO)

# Initialize DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('config_table')

# Valkey configuration
VALKEY_HOST = os.getenv("VALKEY_HOST", "config-valkey-vfeb3i.serverless.use1.cache.amazonaws.com")
VALKEY_PORT = 6379
USE_TLS = True
VALKEY_TIMEOUT = 2  # seconds

# ------------------- Valkey Connection Helpers ------------------- #
async def create_valkey_client() -> Optional[GlideClusterClient]:
    """Create and return a Valkey client with proper timeout settings."""
    config = GlideClusterClientConfiguration(
        addresses=[NodeAddress(VALKEY_HOST, VALKEY_PORT)],
        use_tls=USE_TLS,
        request_timeout=VALKEY_TIMEOUT
    )
    try:
        return await asyncio.wait_for(
            GlideClusterClient.create(config),
            timeout=VALKEY_TIMEOUT
        )
    except (TimeoutError, RequestError, ConnectionError, asyncio.TimeoutError) as e:
        logger.error(f"Valkey connection failed: {e}")
        return None

async def close_valkey_client(client: Optional[GlideClusterClient]) -> None:
    """Safely close a Valkey client."""
    if client:
        try:
            await asyncio.wait_for(client.close(), timeout=1)
        except (ClosingError, asyncio.TimeoutError) as e:
            logger.warning(f"Error closing Valkey client: {e}")

# ------------------- Valkey Operations ------------------- #
async def store_to_valkey(config_key: str, config_value: Dict[str, Any]) -> bool:
    """Store data in Valkey with proper error handling."""
    client = None
    try:
        client = await create_valkey_client()
        if not client:
            return False
            
        value_str = json.dumps(config_value)
        await asyncio.wait_for(
            client.set(config_key, value_str),
            timeout=VALKEY_TIMEOUT
        )
        logger.info(f"Stored in Valkey: {config_key}")
        return True
        
    except (TimeoutError, RequestError, ConnectionError, asyncio.TimeoutError) as e:
        logger.error(f"Valkey store error: {e}")
        return False
    finally:
        await close_valkey_client(client)

async def get_from_valkey(config_key: str) -> Optional[Dict[str, Any]]:
    """Retrieve data from Valkey with proper error handling."""
    client = None
    try:
        client = await create_valkey_client()
        if not client:
            return None
            
        value = await asyncio.wait_for(
            client.get(config_key),
            timeout=VALKEY_TIMEOUT
        )
        
        if value is None:
            return None
            
        value_str = value.decode("utf-8") if isinstance(value, bytes) else str(value)
        return json.loads(value_str)
        
    except (TimeoutError, RequestError, ConnectionError, json.JSONDecodeError, asyncio.TimeoutError) as e:
        logger.error(f"Valkey get error: {e}")
        return None
    finally:
        await close_valkey_client(client)

# ------------------- Lambda Handler ------------------- #
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Handle API Gateway requests."""
    logger.info(f"Received event: {json.dumps(event)}")
    
    method = event.get("httpMethod", "").upper()
    
    try:
        if method == "POST":
            return handle_post(event)
        elif method == "GET":
            return handle_get(event)
        elif method == "PATCH":
            return handle_patch(event)
        else:
            return {
                "statusCode": 405,
                "body": json.dumps({"error": "Method not allowed"})
            }
            
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON: {e}")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid JSON format"})
        }
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error"})
        }

# ------------------- Request Handlers ------------------- #
def handle_post(event: Dict[str, Any]) -> Dict[str, Any]:
    """Handle POST requests to create new items."""
    try:
        body = json.loads(event.get("body", "{}"))
    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid JSON body"})
        }
    
    config_key = body.get("config_key")
    config_value = body.get("config_value")
    
    if not config_key or not config_value:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "config_key and config_value are required"})
        }
    
    # Store in DynamoDB
    try:
        table.put_item(
            Item={'key': config_key, 'value': config_value},
            ConditionExpression="attribute_not_exists(#key)",
            ExpressionAttributeNames={"#key": "key"}
        )
    except boto3.client("dynamodb").exceptions.ConditionalCheckFailedException:
        return {
            "statusCode": 409,
            "body": json.dumps({"error": "Item already exists"})
        }
    except Exception as e:
        logger.error(f"DynamoDB put error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Failed to store in DynamoDB"})
        }
    
    # Store in Valkey
    success = asyncio.get_event_loop().run_until_complete(
        store_to_valkey(config_key, config_value)
    )
    
    if not success:
        logger.warning(f"Failed to store in Valkey, but DynamoDB succeeded for key: {config_key}")
    
    return {
        "statusCode": 200 if success else 207,  # 207 = Multi-status
        "body": json.dumps({
            "message": "Stored successfully" if success else "Stored in DynamoDB but failed in Valkey",
            "stored_in_dynamodb": True,
            "stored_in_valkey": success
        })
    }

def handle_get(event: Dict[str, Any]) -> Dict[str, Any]:
    """Handle GET requests to retrieve items."""
    params = event.get("queryStringParameters", {})
    config_key = params.get("config_key") or params.get("key")
    
    if not config_key:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing config_key parameter"})
        }
    
    # Try Valkey first
    valkey_result = asyncio.get_event_loop().run_until_complete(
        get_from_valkey(config_key)
    )
    
    if valkey_result is not None:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "key": config_key,
                "value": valkey_result,
                "source": "valkey"
            })
        }
    
    # Fall back to DynamoDB
    try:
        response = table.get_item(Key={'key': config_key})
        item = response.get('Item')
        
        if item:
            # Cache the result in Valkey for future requests
            asyncio.get_event_loop().run_until_complete(
                store_to_valkey(config_key, item['value'])
            )
            
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "key": item['key'],
                    "value": item['value'],
                    "source": "dynamodb"
                })
            }
        else:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": "Item not found"})
            }
    except Exception as e:
        logger.error(f"DynamoDB get error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Failed to retrieve item"})
        }

def handle_patch(event: Dict[str, Any]) -> Dict[str, Any]:
    """Handle PATCH requests to update items."""
    try:
        body = json.loads(event.get("body", "{}"))
    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid JSON body"})
        }
    
    config_key = body.get("config_key")
    updated_values = body.get("config_value")
    
    if not config_key or not isinstance(updated_values, dict):
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "config_key and config_value (object) are required"})
        }
    
    # Update DynamoDB
    try:
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
            ExpressionAttributeValues=expression_attribute_values,
            ConditionExpression="attribute_exists(#key)",
            ExpressionAttributeNames={"#key": "key"}
        )
    except boto3.client("dynamodb").exceptions.ConditionalCheckFailedException:
        return {
            "statusCode": 404,
            "body": json.dumps({"error": "Item not found"})
        }
    except Exception as e:
        logger.error(f"DynamoDB update error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Failed to update in DynamoDB"})
        }
    
    # Get the full current value from DynamoDB to store in Valkey
    try:
        response = table.get_item(Key={'key': config_key})
        current_value = response['Item']['value']
    except Exception as e:
        logger.error(f"Failed to get current value from DynamoDB: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Failed to retrieve current value"})
        }
    
    # Update Valkey
    success = asyncio.get_event_loop().run_until_complete(
        store_to_valkey(config_key, current_value)
    )
    
    return {
        "statusCode": 200 if success else 207,  # 207 = Multi-status
        "body": json.dumps({
            "message": "Updated successfully" if success else "Updated in DynamoDB but failed in Valkey",
            "updated_in_dynamodb": True,
            "updated_in_valkey": success
        })
    }