import boto3
import csv
import io
import urllib.parse

s3 = boto3.client("s3")

DEST_BUCKET = "csv-processed-data--bucket"
DEST_PREFIX = "clean/"

def lambda_handler(event, context):

    for record in event["Records"]:

        src_bucket = record["s3"]["bucket"]["name"]
        src_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        obj = s3.get_object(Bucket=src_bucket, Key=src_key)
        content = obj["Body"].read().decode("utf-8")

        reader = csv.DictReader(io.StringIO(content))

        output_buffer = io.StringIO()
        writer = csv.DictWriter(output_buffer, fieldnames=reader.fieldnames)

        writer.writeheader()

        for row in reader:
            cleaned = {k: v.strip() if isinstance(v, str) else v for k, v in row.items()}
            writer.writerow(cleaned)

        file_name = src_key.split("/")[-1]
        dest_key = DEST_PREFIX + file_name

        s3.put_object(
            Bucket=DEST_BUCKET,
            Key=dest_key,
            Body=output_buffer.getvalue()
        )
