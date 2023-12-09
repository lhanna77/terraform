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
		new_file_name = datetime.now().strftime("%Y_%m_%d") + '_' + file_name
		new_directory = "lhannah-sink-state-backup-dev/state_backup/"

		# Name of the object to be stored in the bucket
		blob = bucket.blob(new_file_name)

		# Name of the object in local file system
		blob.upload_from_filename(file_path)

		print(f"{file_name} pushed to {blob.id} succesfully.")
  
		#move_to_dir(bucket, blob, new_file_name, new_directory)
  
		#print(f"{file_name} moved to {new_directory} succesfully.")

	except Exception as e:
		return f"Error - Storage Bucket - {e}"

	return blob

# upload_to_storage_bucket('terraform_tfstate', "C:/Users/lehannah/OneDrive - Monster_AD/Monster Files/Learning/Terraform/Udemy/terraform/terraform.tfstate.backup", "lhannah-sink-dev")

def move_to_dir(bucket, blob, object_name, new_directory):

	# Define the new destination
	new_blob = bucket.blob(new_directory + object_name)

	# Copy the blob to the new destination
	new_blob.rewrite(blob)