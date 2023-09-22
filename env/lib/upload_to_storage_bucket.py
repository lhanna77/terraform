from google.cloud import storage
from datetime import datetime

def upload_to_storage_bucket(file_name, file_path, bucket_id):
	"""
	Uploads a file to a storage bucket from a file location.
	"""

	try:
		# Setting credentials using the downloaded JSON file
		client = storage.Client()

		# Creating bucket object
		bucket = client.get_bucket(bucket_id)

		# Name of the object to be stored in the bucket
		blob = bucket.blob(datetime.now().strftime("%Y_%m_%d") + '_' + file_name)

		# Name of the object in local file system
		blob.upload_from_filename(file_path)

		print(f"{file_name} pushed to {blob.id} succesfully.")

	except Exception as e:
		return f"Error - Storage Bucket - {e}"

	return blob