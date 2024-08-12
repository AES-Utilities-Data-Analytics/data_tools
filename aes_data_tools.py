import os
import zipfile
from google.cloud import storage
from google.colab import auth

# Section 1: Install Dependencies
def install_dependencies():
    os.system("curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg")
    print("Get Google Key")
    os.system('echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt gcsfuse-bionic main" | tee /etc/apt/sources.list.d/gcsfuse.list')
    print("Add the package to list")
    os.system("apt -qq update")
    print("Update the list")
    os.system("apt -qq install gcsfuse")
    print("Package #1 installed")
    os.system("pip -q install pyspark")
    print("Package #2 installed")

# Section 1: GCP Log in
def google_login():
    auth.authenticate_user()
    print('Authenticated')


# Section 2: Map the GCS Bucket as Local Resource
def map_gcs_bucket(bucket_name, endpoint_folder):
    os.makedirs(endpoint_folder, exist_ok=True)
    os.system(f'gcsfuse --implicit-dirs {bucket_name} {endpoint_folder}')

# Section 4: Upload Folder to GCS
def upload_folder_to_gcs(local_folder, bucket_name, destination_folder):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    for root, dirs, files in os.walk(local_folder):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, local_folder)
            destination_blob_name = os.path.join(destination_folder, relative_path)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(local_file_path)
            print(f'File {local_file_path} uploaded to {destination_blob_name}.')

# Section 5: Create ZIP file from folder
def create_zip_from_folder(parquet_folder, zip_file):
    with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(parquet_folder):
            for file in files:
                file_path = os.path.join(root, file)
                zipf.write(file_path, os.path.relpath(file_path, parquet_folder))
    print(f"ZIP file created at {zip_file}")

# Section 6: Get file size in MB
def get_file_size_in_mb(file_path):
    file_size_bytes = os.path.getsize(file_path)
    file_size_mb = file_size_bytes / (1024 * 1024)
    print(f"ZIP file size: {file_size_mb:.2f} MB")
    return file_size_mb

# Section 7: Upload ZIP file to GCS
def upload_file_to_gcs(local_zip_path, bucket_name, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_zip_path)
    print(f'File {local_zip_path} uploaded to {destination_blob_name}.')


# Example usage:
'''
if __name__ == "__main__":
    # Install required dependencies
    install_dependencies()

    # Map GCS bucket to a local folder
    bucket_name = 'aes-datahub-0001-smartgrid-indiana-landing'
    endpoint_folder = 'datasource'
    map_gcs_bucket(bucket_name, endpoint_folder)

    # Initialize Spark
    spark = initialize_spark()

    # Upload a folder to GCS
    local_folder = './acxiom_dict.parquet/'
    destination_folder = 'acxiom_dict.parquet/'
    upload_folder_to_gcs(local_folder, bucket_name, destination_folder)

    # Create ZIP file from folder
    parquet_folder = './Acxiom.parquet/'
    zip_file = './Acxiom.parquet.zip'
    create_zip_from_folder(parquet_folder, zip_file)

    # Get file size of ZIP
    file_size_mb = get_file_size_in_mb(zip_file)

    # Upload ZIP file to GCS
    local_zip_path = './Acxiom.parquet.zip'
    destination_blob_name = 'acxiom_residential/Acxiom.parquet.zip'
    upload_zip_to_gcs(local_zip_path, bucket_name, destination_blob_name)

'''
