# connections/aws_config.py

# --- AWS Secrets Manager Configuration (Used by secrets_manager.py) ---
# The name of the secret storing your S3 access_key and secret_key credentials
S3_SECRET_NAME = "capstone/spark/s3-credentials" 
# The AWS region where your secrets reside
AWS_REGION = "us-east-1" 