import os, uuid
import env
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

#===============================================#
#	Azure
#===============================================#


# create a azure blob container
def connect_to_blob_service_client(connection_str:str):
	blob_service_client = BlobServiceClient.from_connection_string(connection_str)
	#container_client = blob_service_client.create_container(container_name)
	print(blob_service_client.get_account_information())
	return(blob_service_client)



# upload blob to an azure blob storage container
def upload_file_to_azure_container(local_file_path:str, container_name:str, connection_str:str):
	blob_service_client = BlobServiceClient.from_connection_string(connection_str)
	blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_path)
	with open(local_file_path, "rb") as data:
		blob_client.upload_blob(data)



