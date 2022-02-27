import os, uuid
from . import env
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError
from typing import Dict
import pandas as pd

#===============================================#
#	Azure
#===============================================#

class MyAzureBlobStorage():
	"""
	An Azure Blob Storage to store my data: text or ginary data	
	parent class definition is at: https://github.com/Azure/azure-sdk-for-python/blob/a5ea4bf7d0decafbe355467ddb0bdd334b9f953b/sdk/storage/azure-storage-blob/azure/storage/blob/_blob_service_client.py#L26
	"""
	def __init__(self, credentials:Dict[str, str]=None):
		"""
		:param dict credentials: can supply coneection_string as the key or other credentials:
			if connection_string is specified, overrides all other parameters to create a BlobServiceClient to connect to the blob storage
			example: credentials = {"connection_string": "XXXXXXX"}
		"""
		if credentials is not None:
			if "connection_string" in credentials.keys(): self.blob_service_client = BlobServiceClient.from_connection_string(credentials["connection_string"])
		else:
			connection_str = env.get_azure_storage_connection_string().strip('"')
			self.blob_service_client = BlobServiceClient.from_connection_string(connection_str)

	def list_container_names(self):
		container_list = [c.name for c in self.blob_service_client.list_containers()]
		return(container_list)
		
	def exists_container(self, container_name:str) -> bool:
		""
		return(container_name in self.list_container_names())

	def create_container(self, container_name:str):
		""
		if not self.exists_container(container_name):
			self.blob_service_client.create_container(container_name)

	def delete_container(self, container_name:str):
		if self.exists_container(container_name): self.blob_service_client.delete_container(container_name)

	def get_container(self, container_name:str) -> ContainerClient:
		"""
		return the container with the name if exists 
		else create a new container with the name and return
		"""
		if not self.exists_container(container_name): 
			self.create_container(container_name)
		c = self.blob_service_client.get_container_client(container_name)
		
		return(c)

	def upload(self, data, container_name:str):
		c = self.get_container(container_name)
		if isinstance(data, str) or isinstance(data, bytes) or isinstance(data, os.PathLike) or isinstance(data, int):
			blob = self.blob_service_client.get_blob_client(container=container_name, blob=data)
			with open(data, "rb") as d:
				blob.upload_blob(d, overwrite=True)
		elif isinstance(data, pd.DataFrame):
			file = 'data_all.csv'
			data.to_csv(file)
			blob = self.blob_service_client.get_blob_client(container=container_name, blob=file)

			with open(file, "rb") as d:
				blob.upload_blob(d, overwrite=True)

			os.remove(file)

	def download_to_dataframe(self, blob_name:str, container_name:str):
		blob = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
		with open(os.getcwd()+"\\data.csv", "wb") as download_file:
			download_file.write(blob.download_blob().readall())

		return(pd.read_csv(os.getcwd()+"\\data.csv"))	


def upload_azure_blob_storage(data, container_name:str=None, azure_credentials:Dict[str, str]=None):
	blobs = MyAzureBlobStorage(azure_credentials)
	if not container_name: container_name="data"
	blobs.upload(data, container_name)

def read_dataframe_azure_blob(blob_name:str, container_name:str, azure_credentials:Dict[str, str]=None):
	blob = MyAzureBlobStorage(azure_credentials)
	try: df = blob.download_to_dataframe(blob_name, container_name)
	except ResourceNotFoundError: df=None
	os.remove(os.getcwd()+"\\data.csv")
	return(df)

# create a azure blob container
def connect_to_blob_service_client():
	"""
	returns a blob_service_client
	:param dict credentials: can supply coneection_string as the key or other credentials:
		example: {"connection_string": }

	"""
	blob_service_client = BlobServiceClient.from_connection_string(connection_str)
	#container_client = blob_service_client.create_container(container_name)
	return(blob_service_client)




# upload blob to an azure blob storage container
def upload_file_to_azure_container(local_file_path:str, container_name:str, connection_str:str):
	blob_service_client = connect_to_blob_service_client(connection_str)
	blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_path)
	with open(local_file_path, "rb") as data:
		blob_client.upload_blob(data)



