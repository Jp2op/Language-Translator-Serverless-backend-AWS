import json
import boto3
import uuid
import os
import re
from datetime import datetime, timezone

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table_name = os.environ.get("DYNAMO_TABLE_NAME")
table = dynamodb.Table(table_name)

def parse_multipart_form_data(body, content_type):
    """
    Parses a multipart/form-data body.
    """
    match = re.search(r"boundary=([^;]+)", content_type)
    if not match:
        raise ValueError("Invalid Content-Type header: missing boundary")
    boundary = match.group(1)

    parts = body.split(b"--" + boundary.encode())

    fields = {}
    for part in parts[1:-1]:  # Exclude the first and last parts (empty or --)
        try:
            part_headers, part_body = part.split(b"\r\n\r\n", 1)
            part_headers = part_headers.decode()

            content_disposition_match = re.search(
                r'Content-Disposition: form-data; name="([^"]+)"(?:; filename="([^"]+)")?', part_headers)
            if not content_disposition_match:
                continue

            name = content_disposition_match.group(1)
            filename = content_disposition_match.group(2)

            if filename:
                fields[name] = {
                    'filename': filename,
                    'content': part_body.rstrip(b"\r\n")  # Remove trailing newline
                }
            else:
                fields[name] = part_body.rstrip(b"\r\n").decode()
        except Exception as e:
            print(f"Error parsing part: {str(e)}")
            continue

    return fields

def log_upload_metadata(file_key, original_filename):
    try:
        timestamp = datetime.now(timezone.utc).isoformat()
        table.put_item(
            Item={
                'file_key': file_key,
                'original_filename': original_filename,
                'status': 'uploaded',
                'upload_time': timestamp,
                'stage': 'upload'
            }
        )
        print(f"Metadata logged to DynamoDB for {file_key}")
    except Exception as e:
        print(f"[WARN] Failed to log metadata: {str(e)}")

def lambda_handler(event, context):
    """
    Handles audio file uploads (MP3) via API Gateway, validates them,
    and uploads them to an S3 bucket with metadata.
    """
    try:
        # --- 1. Parse the Incoming Request ---
        body = event['body'].encode()  # Always send the body as bytes (no Base64 decoding)

        # --- 2. Handle Headers and Content-Type ---
        headers = {k.lower(): v for k, v in event['headers'].items()}
        content_type = headers.get('content-type')
        if not content_type:
            raise ValueError("Missing 'Content-Type' header.")

        # --- 3. Parse Multipart/Form-Data ---
        fields = parse_multipart_form_data(body, content_type)

        # --- 4. Extract Required Fields ---
        if 'file' not in fields:
            raise ValueError("Missing file in the request.")
        
        file_field = fields['file']
        file_content = file_field['content']
        file_name = file_field['filename']

        if not file_content:
            raise ValueError("File content is empty.")

        # Debugging
        print(f"File name: {file_name}")
        print(f"File content length: {len(file_content)}")

        # --- 5. Generate Unique File Name ---
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        unique_id = str(uuid.uuid4())[:4]

        s3_file_key = f"{timestamp}_{unique_id}.mp3"

        # --- 6. Upload to S3 with Metadata ---
        s3.put_object(
            Bucket=os.environ['INPUT_BUCKET'],
            Key=s3_file_key,
            Body=file_content,
            ContentType='audio/mpeg',
            Metadata={
                'upload_time': datetime.now(timezone.utc).isoformat()
            }
        )
        print(f"File uploaded to S3: {s3_file_key}")

        # --- 7. Log Metadata to DynamoDB ---
        log_upload_metadata(s3_file_key, file_name)

        # --- 8. Return Success Response ---
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'  # Adjust CORS as needed for your frontend
            },
            'body': json.dumps({
                'message': 'File uploaded successfully.',
                'file_key': s3_file_key
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
