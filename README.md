# mongoBackupToS3
 mongodb backup and upload to s3 bucket
 
 Last tested on Python 3.6.8
 
 Script to run: `mongoBackupAndRestoreS3.py`
 
 Usage: 
 ```
 mongoBackupAndRestoreS3.py [OPTIONS]

  Options:
  --op TEXT        type of operation: backup or restore  [required]
  --s3bucket TEXT  aws bucket name  [required]
  --help           Show this message and exit.
  ```
 It will ask for aws access key id and secret access key as input. You can also configure these as environment variables under `aws_access_key_id` and `aws_secret_access_key`; this helps them configurable via jenkins as well.
 
 Example usage: `python mongoBackupAndRestoreS3.py --op backup --s3bucket 'mybucket'`
