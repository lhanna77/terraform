from google.cloud import secretmanager
import os
import pyzipper
import secrets
import string
import shutil

def get_latest_secret(project_id, secret_id):
    client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{secret_id}"
    response = client.access_secret_version(request={"name": f"{secret_name}/versions/latest"})
    payload = response.payload.data.decode("UTF-8")
    
    return payload

def add_secret_to_json(secret, json_file):
    
    file_path = f"key_rotation_export/{json_file}"
    
    with open(file_path, 'w') as file:
        file.write(secret)

def process_secrets(project_id, secret_ids, json_files):
    for secret_id, json_file in zip(secret_ids, json_files):
        
        print(secret_id)
        
        latest_secret = get_latest_secret(project_id, secret_id)
        add_secret_to_json(latest_secret, json_file)
        
def zip_json_files_with_password(json_files_pw, pw):
    for j, p in zip(json_files_pw, pw):
        j = f"key_rotation_export/{j}"
        z = j.replace('.json','.zip')
        
        with pyzipper.AESZipFile(z, 'w', compression=pyzipper.ZIP_LZMA,encryption=pyzipper.WZ_AES) as zf:
            zf.setpassword(p.encode())
            zf.write(j)

def gen_random_pw_list(list_len):
    
    pw_len = 10
    pw_list = []
    
    chars = string.ascii_letters + string.digits + string.punctuation
    
    for i in range(list_len):
        pw_list.append(''.join(secrets.choice(chars) for _ in range(pw_len)))
    
    return pw_list

def main():

    export_path = "key_rotation_export"

    if os.path.exists(export_path):
        shutil.rmtree(export_path)

    if not os.path.exists(export_path):
        os.makedirs(export_path)

    project_id = "mstr-datastage-dev-df70" # "mstr-datastage-dev-df70" / "mstr-datastage-qa-c03b" / "mstr-datastage-prd-95ee"

    env = env = project_id.split('-')[2]
    month_year = "mar2024"

    print(f"You are processing json files for {env}\n")

    if env != 'prd':

        secret_ids = ["service-account-keys-job_distribution_tool","service-account-keys-dnb_achilles","service-account-keys-gcp_publisher","service-account-keys-amplitude","service-account-keys-onprem"]
        json_files = [f"{env}_{s}_{month_year}.json" for s in secret_ids]
        pw = gen_random_pw_list(len(secret_ids))

    elif  env == 'prd':

        secret_ids = ["service-account-keys-job_distribution_tool","service-account-keys-dnb_achilles","service-account-keys-auth0","service-account-keys-joveo","service-account-keys-amplitude","service-account-keys-onprem"]
        json_files = [f"{env}_{s}_{month_year}.json" for s in secret_ids]
        pw = gen_random_pw_list(len(secret_ids))

    process_secrets(project_id, secret_ids, json_files)

    print(f"\n")

    for j,p in zip(json_files, pw):
        print(f"File: {j} - password: {p}")
        
    zip_json_files_with_password(json_files, pw)

if __name__ == "__main__":
    main()