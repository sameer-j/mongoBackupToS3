import boto3
import subprocess
import os, shutil
import click
from datetime import datetime
import tarfile

class DataBackupAndRestore:
    """
    Backup database to AWS and Restore database from AWS
    Current implementation does it for all the databases, including the system databases
    TODO: Restore
    """

    def __init__(self, aws_access_key_id, aws_secret_access_key):
        """Initialize the object
        
        Arguments:
            aws_access_key_id {string} -- AWS access key id
            aws_secret_access_key {string} -- AWS secret access key
        """
        self.s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    
    def backup(self):
        """
        1. backup using mongodump with gzip format
        2. compress the backup folder
        
        Returns:
            filename {string} -- backup file name
        """
        # mongodump compressed
        print('Making mongodump in gz...')
        output = subprocess.check_output(['mongodump', '--out=databackup-temp/databackup', '--gzip'], encoding='utf-8')
        print(output)
        # compress the backup folder with a file name
        current_datetime = datetime.now()
        filename = f'databackup--{current_datetime.strftime("%d-%m-%Y--%H-%M")}.tar.gz'
        make_tarfile(filename, 'databackup-temp/databackup')
        return filename

    def upload_to_aws(self, local_file, bucket, s3_file):
        """uploads given file path to given bucket with given filename for s3
        
        Arguments:
            local_file {string} -- local file path
            bucket {string} -- s3 bucket name
            s3_file {string} -- s3 file name
        Returns:
            status {boolean} -- True is upload successful
        """
        try:
            print(f'Uploading {local_file}...')
            statinfo = os.stat(local_file)
            bar = click.progressbar(length=statinfo.st_size, label='Uploading', show_pos=True, show_percent=True)
            self.s3_client.upload_file(local_file, bucket, s3_file, Callback=bar.update)
            print("Upload Successful")
            return True
        except FileNotFoundError:
            print("The file was not found")
            return False
        except Exception as e:
            print(f"Exception: {e}")
            raise e
            return False

    def clear_older_backup(self, bucket, keep_count = 10):
        """clears older backup files and keeps max files determined by keep_count.
        
        Keyword Arguments:
            bucket {str} -- bucket name
            keep_count {int} -- max backup files to keep (default: {10})
        """
        print(f'Clearing older backups from aws, keeping {keep_count}')
        get_last_modified = lambda obj: int(obj['LastModified'].strftime('%Y%m%d%H%M%S'))
        objs = self.s3_client.list_objects_v2(Bucket=bucket)['Contents']
        files = [obj['Key'] for obj in sorted(objs, key=get_last_modified)]
        for file in files[keep_count:]:
            self.s3_client.delete_object(Bucket=bucket, Key=file)
        print(f'Deleted backups {files[keep_count:]}')

def clearBackupFiles(folder):
    print('Clearing temp folder containing backup files...')
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))
    shutil.rmtree(folder)
    print(f'Deleted {folder}')

def make_tarfile(output_filename, source_dir):
    """Utility function to make tar gz file of the given source_dir
    
    Arguments:
        output_filename {string} -- filename of the gzip file
        source_dir {string} -- directory name to be gzipped
    """
    with tarfile.open(os.path.join('databackup-temp', output_filename), "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))

@click.command()
@click.option('--op', required=True, type=str, help="type of operation: backup or restore")
@click.option('--s3bucket', required=True, type=str, help="aws bucket name")
def backupAndRestoreAutomation(op, s3bucket):
    aws_access_key_id = os.getenv('aws_access_key_id')# get from env variable in jenkins
    aws_secret_access_key = os.getenv('aws_secret_access_key')# get from env variable in jenkins
    s3_bucket = s3bucket
    bkpRstr = DataBackupAndRestore(aws_access_key_id, aws_secret_access_key)
    if op == 'backup':
        try:
            backup_filename = bkpRstr.backup()
            bkpRstr.upload_to_aws(local_file=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'databackup-temp', backup_filename), bucket = s3_bucket, s3_file=f'{backup_filename}')
            bkpRstr.clear_older_backup(s3_bucket)
        except Exception as e:
            print(f'Exception while backup and upload: {e}')
            raise e
        finally:
            # clean up mongodump and compressed backup folder
            clearBackupFiles(os.path.join(os.path.dirname(__file__), 'databackup-temp'))
    elif op == 'restore':
        print('Not implemented yet!')

if __name__ == "__main__":
    backupAndRestoreAutomation()
