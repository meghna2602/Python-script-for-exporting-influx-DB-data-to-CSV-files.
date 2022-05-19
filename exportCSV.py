import os
import csv
from influxdb import InfluxDBClient
from datetime import datetime
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.storage.filedatalake import DataLakeServiceClient

host = "ip/url"
port = "8086"
username = "DB user name"
password = "DB password"
dbname = "DB name"
time_length = "1w"
end_time = 'now()'
filtered_str = ' '
filtered = [x.strip() for x in filtered_str.split(',')]
client = InfluxDBClient(host, port, username, password, dbname)
# first we get list of all measurements in the selected db to dump them
query = 'show measurements'
result = client.query(query)

now1 = datetime.now()
foldername = now1.strftime("%Y-%m-%d")
for measurements in result:
    for measure in measurements:
        measure_name = measure['name']
        # get list of all fields for the measurement to build the CSV header
        query = 'show field keys from "' + measure_name + '"'
        names = ['host', 'time', 'readable_time']
        fields_result = client.query(query)
        for field in fields_result:
            for pair in field:
                name = pair['fieldKey']
                if name in filtered: continue
                names.append(name)
        now1 = datetime.now()
        filename = "CSVFiles/" + foldername + "/" + measure_name + '.csv'
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        # finally, request all data for all measurements in given timeframe and dump them as CSV rows to files
        with open(filename, 'w') as file:
            writer = csv.DictWriter(file, names, delimiter=',', lineterminator='\n', extrasaction='ignore')
            writer.writeheader()
            query = """select * from "{}" where time > {} - {} AND time < {} """.format(measure_name, end_time,time_length, end_time)
            print(query)
            # https://docs.influxdata.com/influxdb/v0.13/guides/querying_data/
            result = client.query(query, epoch='ms')
            for point in result:
                for item in point:
                    ms = item['time'] / 1000
                    d = datetime.fromtimestamp(ms)
                    item['readable_time'] = d.isoformat('T') + 'Z'
                    writer.writerow(item)

MY_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=*********;" \
                       "AccountKey=**************;" \
                       "EndpointSuffix=core.windows.net"
#change the path over here
path = r"C:\Users\GoBrilliant\PycharmProjects\pythonProject\CSVFiles"
LOCAL_IMAGE_PATH = os.path.join(path,foldername)

class AzureBlobFileUploader:

    def __init__(self):
        print("Initializing AzureBlobFileUploader")

        # Initialize the connection to Azure storage account
        self.blob_service_client = BlobServiceClient.from_connection_string(MY_CONNECTION_STRING)

    def upload_all_files_in_folder(self):
        # Get all files with jpg extension and exclude directories
        all_file_names = [f for f in os.listdir(LOCAL_IMAGE_PATH)
                          if os.path.isfile(os.path.join(LOCAL_IMAGE_PATH, f)) and ".csv" in f]

        # Upload each file
        for file_name in all_file_names:
            self.upload_file(file_name)

    def upload_file(self,file_name):
        service_client = DataLakeServiceClient(
            account_url="{}://{}.dfs.core.windows.net".format("https", "*******"),
            credential="***********************")
        file_system_client = service_client.get_file_system_client(file_system="test-container")
        directory_client = file_system_client.create_directory(foldername)
        file_client = directory_client.create_file(file_name)
        upload_file_path = os.path.join(LOCAL_IMAGE_PATH,file_name)
        local_file = open(upload_file_path, 'r')  # Change the Path over here !!!
        file_contents = local_file.read()
        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
        file_client.flush_data(len(file_contents))

# Initialize class and upload files
azure_blob_file_uploader = AzureBlobFileUploader()
azure_blob_file_uploader.upload_all_files_in_folder()

blob_service_client = BlobServiceClient.from_connection_string(
        "DefaultEndpointsProtocol=https;AccountName=******;AccountKey=*******************************;EndpointSuffix=core.windows.net")
service_client = DataLakeServiceClient(
        account_url="{}://{}.dfs.core.windows.net".format("https", "********"),
        credential="*************************")

    # Get the Blob Names from the Container
container_client = blob_service_client.get_container_client("test-container")
blobs_list = container_client.list_blobs()
    # Check the Blob name is present or not
for blob in blobs_list:
    print(blob.name)
container_client.delete_blobs(foldername)
