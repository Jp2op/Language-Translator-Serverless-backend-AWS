import boto3
import json
import uuid

polly = boto3.client('polly')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        translated_text = event['translated_text']
        input_bucket_name = event['bucket']
        output_file_name = event['output_file'] or f"{uuid.uuid4()}_speech.mp3"  # Ensure uniqueness

        output_bucket_name = 'realtime-language-translation-output-bucket'  # Replace with your output bucket name

        # Use Polly to synthesize the speech
        response = polly.synthesize_speech(
            Text=translated_text,
            OutputFormat='mp3',
            VoiceId='Joanna'  # Adjust the voice as needed
        )

        # Save the audio stream to the specified output S3 bucket
        if 'AudioStream' in response:
            s3.put_object(
                Bucket=output_bucket_name,
                Key=output_file_name,
                Body=response['AudioStream'].read(),
                ContentType='audio/mpeg'
            )
            print(f"Audio saved to {output_bucket_name}/{output_file_name}")
        else:
            print("No AudioStream found in Polly response.")
            return {
                'statusCode': 500,
                'body': json.dumps("No AudioStream found in Polly response.")
            }

        return {
            'statusCode': 200,
            'body': json.dumps(f"Audio saved to {output_bucket_name}/{output_file_name}")
        }
    except KeyError as e:
        print(f"Missing key in event: {str(e)}")
        return {
            'statusCode': 400,
            'body': json.dumps(f"Missing key: {str(e)}")
        }
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"An error occurred: {str(e)}")
        }
