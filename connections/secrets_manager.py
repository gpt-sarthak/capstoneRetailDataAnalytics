# connections/secrets_manager.py

import boto3
import json
from botocore.exceptions import ClientError
# We rely on the project's root configuration for the AWS region
from ..config import AWS_REGION 

def get_secret(secret_name: str) -> dict:
    """
    Retrieves and parses a JSON-formatted secret stored in AWS Secrets Manager.
    
    Args:
        secret_name: The name or ARN of the secret (e.g., 'capstone/spark/s3-credentials').
        
    Returns:
        A dictionary containing the secret key-value pairs (e.g., {'access_key': '...', 'secret_key': '...'}).
        
    Raises:
        ClientError: If the secret cannot be retrieved due to permissions or resource not found.
    """
    
    # Create a Secrets Manager client using the configured region
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=AWS_REGION)
    
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # Catch common AWS exceptions (e.g., ResourceNotFoundException, AccessDeniedException)
        print(f"ERROR retrieving secret {secret_name} in {AWS_REGION}: {e}")
        raise e

    # Decrypts secret. We assume it is stored as a JSON string.
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
    else:
        # Fallback for binary secrets (less common for credentials)
        import base64
        secret = base64.b64decode(get_secret_value_response['SecretBinary']).decode('utf-8')

    # Convert the JSON string into a usable Python dictionary
    return json.loads(secret)


if __name__ == "__main__":
    # --- Example Usage (Requires a valid config.py and AWS credentials setup) ---
    print("--- Testing Secrets Manager Retrieval ---")
    
    # NOTE: You need to set the SECRET_NAME constant for a successful local test
    TEST_SECRET_NAME = "capstone/spark/s3-credentials" 
    
    try:
        # Ensure your local environment has permission to read this secret (via AWS CLI config or Role)
        creds = get_secret(TEST_SECRET_NAME)
        
        print(f"Successfully retrieved secret '{TEST_SECRET_NAME}'.")
        print(f"Access Key ID: {creds['access_key'][:4]}...{creds['access_key'][-4:]}")
        print(f"Secret Key: {creds['secret_key'][:4]}...{creds['secret_key'][-4:]} (Key shown truncated for security)")
        
    except Exception as e:
        print(f"TEST FAILED: Please verify that: \n1. Your local AWS CLI credentials are set up. \n2. Your IAM user has 'secretsmanager:GetSecretValue' permissions on the secret. \n3. The secret name '{TEST_SECRET_NAME}' and region '{AWS_REGION}' are correct in config.py.")