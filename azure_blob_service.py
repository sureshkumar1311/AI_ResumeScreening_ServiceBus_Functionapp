"""
Azure Blob Storage service for handling file uploads and downloads
"""

from azure.storage.blob import BlobServiceClient, ContentSettings
from config import settings
from typing import Optional
import uuid
from datetime import datetime, timedelta
import re


class AzureBlobService:
    """Service for Azure Blob Storage operations"""
    
    def __init__(self):
        """Initialize blob service client"""
        self.blob_service_client = BlobServiceClient.from_connection_string(
            settings.AZURE_STORAGE_CONNECTION_STRING
        )
        self._ensure_containers_exist()
    
    def _ensure_containers_exist(self):
        """Ensure required containers exist"""
        containers = [
            settings.AZURE_STORAGE_CONTAINER_JOB_DESCRIPTIONS,
            settings.AZURE_STORAGE_CONTAINER_RESUMES
        ]
        
        for container_name in containers:
            try:
                container_client = self.blob_service_client.get_container_client(container_name)
                if not container_client.exists():
                    container_client.create_container()
            except Exception as e:
                print(f"Error ensuring container {container_name} exists: {str(e)}")
    
    async def upload_file(
        self,
        file_content: bytes,
        blob_name: str,
        content_type: Optional[str] = None
    ) -> str:
        """
        Upload file to blob storage and return SAS URL
        
        Args:
            file_content: File content as bytes
            blob_name: Name/path for the blob
            content_type: MIME type of the file
        
        Returns:
            SAS URL of the uploaded blob (valid for 365 days)
        """
        try:
            from azure.storage.blob import generate_blob_sas, BlobSasPermissions
            from datetime import datetime, timedelta
            
            # Determine container based on blob path
            if blob_name.startswith("job-descriptions/"):
                container_name = settings.AZURE_STORAGE_CONTAINER_JOB_DESCRIPTIONS
            elif blob_name.startswith("resumes/"):
                container_name = settings.AZURE_STORAGE_CONTAINER_RESUMES
            else:
                container_name = settings.AZURE_STORAGE_CONTAINER_RESUMES
            
            # Get blob client
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            
            # Set content settings
            content_settings = ContentSettings(content_type=content_type) if content_type else None
            
            # Upload blob
            blob_client.upload_blob(
                file_content,
                overwrite=True,
                content_settings=content_settings
            )
            
            # Generate SAS token (valid for 365 days)
            sas_token = generate_blob_sas(
                account_name=self.blob_service_client.account_name,
                container_name=container_name,
                blob_name=blob_name,
                account_key=self._get_account_key(),
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(days=365)
            )
            
            # Return blob URL with SAS token
            return f"{blob_client.url}?{sas_token}"
        
        except Exception as e:
            raise Exception(f"Failed to upload file to blob storage: {str(e)}")
    
    def _get_account_key(self) -> str:
        """Extract account key from connection string"""
        try:
            conn_str = settings.AZURE_STORAGE_CONNECTION_STRING
            parts = dict(item.split('=', 1) for item in conn_str.split(';') if '=' in item)
            return parts.get('AccountKey', '')
        except Exception as e:
            raise Exception(f"Failed to extract account key: {str(e)}")
    
    async def download_file(self, blob_url: str) -> bytes:
        """
        Download file from blob storage using direct URL
        
        Args:
            blob_url: Complete blob URL (e.g., https://account.blob.core.windows.net/container/path/file.pdf)
        
        Returns:
            File content as bytes
        """
        try:
            # Parse the blob URL to extract container and blob path
            # URL format: https://{account}.blob.core.windows.net/{container}/{blob_path}
            
            # Remove query parameters (SAS token) if present
            clean_url = blob_url.split('?')[0]
            
            # Parse URL
            # Example: https://airesumeagentblob.blob.core.windows.net/resume-eventgrid/job-id/file.pdf
            match = re.match(r'https://([^.]+)\.blob\.core\.windows\.net/([^/]+)/(.+)$', clean_url)
            
            if not match:
                raise ValueError(f"Invalid blob URL format: {blob_url}")
            
            account_name = match.group(1)
            container_name = match.group(2)
            blob_path = match.group(3)
            
            print(f"      Downloading from:")
            print(f"         Account: {account_name}")
            print(f"         Container: {container_name}")
            print(f"         Blob Path: {blob_path}")
            
            # Get blob client using the parsed information
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_path
            )
            
            # Download blob
            download_stream = blob_client.download_blob()
            content = download_stream.readall()
            
            return content
        
        except Exception as e:
            raise Exception(f"Failed to download file from blob storage: {str(e)}")
    
    async def delete_file(self, blob_url: str) -> bool:
        """
        Delete file from blob storage
        
        Args:
            blob_url: URL of the blob
        
        Returns:
            True if deleted successfully
        """
        try:
            # Parse blob URL
            clean_url = blob_url.split('?')[0]
            match = re.match(r'https://([^.]+)\.blob\.core\.windows\.net/([^/]+)/(.+)$', clean_url)
            
            if not match:
                raise ValueError(f"Invalid blob URL format: {blob_url}")
            
            container_name = match.group(2)
            blob_path = match.group(3)
            
            # Get blob client
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_path
            )
            
            # Delete blob
            blob_client.delete_blob()
            return True
        
        except Exception as e:
            print(f"Failed to delete file from blob storage: {str(e)}")
            return False
    
    async def generate_sas_url(
        self,
        blob_url: str,
        expiry_hours: int = 24
    ) -> str:
        """
        Generate SAS URL for temporary access
        
        Args:
            blob_url: URL of the blob
            expiry_hours: Hours until SAS token expires
        
        Returns:
            SAS URL with temporary access
        """
        try:
            from azure.storage.blob import generate_blob_sas, BlobSasPermissions
            
            # Parse blob URL
            clean_url = blob_url.split('?')[0]
            match = re.match(r'https://([^.]+)\.blob\.core\.windows\.net/([^/]+)/(.+)$', clean_url)
            
            if not match:
                raise ValueError(f"Invalid blob URL format: {blob_url}")
            
            container_name = match.group(2)
            blob_path = match.group(3)
            
            # Generate SAS token
            sas_token = generate_blob_sas(
                account_name=self.blob_service_client.account_name,
                container_name=container_name,
                blob_name=blob_path,
                account_key=self._get_account_key(),
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(hours=expiry_hours)
            )
            
            return f"{clean_url}?{sas_token}"
        
        except Exception as e:
            raise Exception(f"Failed to generate SAS URL: {str(e)}")