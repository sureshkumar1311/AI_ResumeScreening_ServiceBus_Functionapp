"""
Azure Cosmos DB service for storing job descriptions, screening results, and users
"""

from azure.cosmos import CosmosClient, PartitionKey, exceptions
from config import settings
from typing import Optional, List, Dict, Any
import uuid
from datetime import datetime


class CosmosDBService:
    """Service for Azure Cosmos DB operations"""
    
    def __init__(self):
        """Initialize Cosmos DB client"""
        self.client = CosmosClient(
            settings.COSMOS_DB_ENDPOINT,
            settings.COSMOS_DB_KEY
        )
        self.database = None
        self.jobs_container = None
        self.screenings_container = None
        self.users_container = None
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize database and containers"""
        try:
            # Create database if not exists
            self.database = self.client.create_database_if_not_exists(
                id=settings.COSMOS_DB_DATABASE_NAME
            )
            
            # Create jobs container if not exists
            # REMOVED offer_throughput for serverless compatibility
            self.jobs_container = self.database.create_container_if_not_exists(
                id=settings.COSMOS_DB_CONTAINER_JOBS,
                partition_key=PartitionKey(path="/user_id")
            )
            
            # Create screenings container if not exists
            self.screenings_container = self.database.create_container_if_not_exists(
                id=settings.COSMOS_DB_CONTAINER_SCREENINGS,
                partition_key=PartitionKey(path="/job_id")
            )
            
            # Create users container if not exists
            self.users_container = self.database.create_container_if_not_exists(
                id=settings.COSMOS_DB_CONTAINER_USERS,
                partition_key=PartitionKey(path="/user_id")
            )
        
        except Exception as e:
            print(f"Error initializing Cosmos DB: {str(e)}")
            raise
    
    # ==================== USER MANAGEMENT ====================
    
    async def create_user(
        self,
        email: str,
        hashed_password: str,
        full_name: str,
        company_name: Optional[str] = None
    ) -> str:
        """
        Create a new user
        
        Args:
            email: User email (unique identifier)
            hashed_password: Hashed password
            full_name: Full name of user
            company_name: Optional company name
        
        Returns:
            User ID
        """
        try:
            user_id = str(uuid.uuid4())
            
            user_data = {
                "id": user_id,
                "user_id": user_id,
                "email": email.lower(),
                "hashed_password": hashed_password,
                "full_name": full_name,
                "company_name": company_name,
                "created_at": datetime.utcnow().isoformat(),
                "is_active": True,
                "total_jobs": 0,
                "total_screenings": 0
            }
            
            self.users_container.create_item(body=user_data)
            return user_id
        
        except Exception as e:
            raise Exception(f"Failed to create user: {str(e)}")
    
    async def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """
        Get user by email address
        
        Args:
            email: User email
        
        Returns:
            User data or None
        """
        try:
            query = "SELECT * FROM c WHERE c.email = @email"
            parameters = [{"name": "@email", "value": email.lower()}]
            
            items = list(self.users_container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True
            ))
            
            if items:
                return items[0]
            return None
        
        except Exception as e:
            raise Exception(f"Failed to retrieve user: {str(e)}")
    
    async def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Get user by user ID
        
        Args:
            user_id: User ID
        
        Returns:
            User data or None
        """
        try:
            item = self.users_container.read_item(
                item=user_id,
                partition_key=user_id
            )
            return item
        
        except exceptions.CosmosResourceNotFoundError:
            return None
        except Exception as e:
            raise Exception(f"Failed to retrieve user: {str(e)}")
    
    async def update_user_stats(self, user_id: str, increment_jobs: int = 0, increment_screenings: int = 0):
        """
        Update user statistics
        
        Args:
            user_id: User ID
            increment_jobs: Number to increment total_jobs by
            increment_screenings: Number to increment total_screenings by
        """
        try:
            user_data = await self.get_user_by_id(user_id)
            if user_data:
                user_data["total_jobs"] = user_data.get("total_jobs", 0) + increment_jobs
                user_data["total_screenings"] = user_data.get("total_screenings", 0) + increment_screenings
                self.users_container.upsert_item(body=user_data)
        
        except Exception as e:
            print(f"Failed to update user stats: {str(e)}")
    
    # ==================== JOB DESCRIPTION MANAGEMENT ====================
    
    async def create_job_description(
        self,
        user_id: str,
        screening_name: str,
        job_description_text: str,
        must_have_skills: List[str],  # Changed from List[Dict] to List[str]
        nice_to_have_skills: List[str],  # Changed from List[Dict] to List[str]
        filename: Optional[str] = None,
        blob_url: Optional[str] = None
    ) -> str:
        """
        Create a new job description entry
        
        Args:
            user_id: ID of the user creating this job
            screening_name: Name/title for this screening
            job_description_text: Extracted or manual job description text
            must_have_skills: List of must-have skill strings (auto-extracted)
            nice_to_have_skills: List of nice-to-have skill strings (auto-extracted)
            filename: Optional original filename or "Manual Entry"
            blob_url: Optional Azure Blob URL for the job description file
        
        Returns:
            Job ID
        """
        try:
            job_id = str(uuid.uuid4())
            
            job_data = {
                "id": job_id,
                "job_id": job_id,
                "user_id": user_id,
                "screening_name": screening_name,
                "job_description_text": job_description_text,
                "filename": filename if filename else "Manual Entry",
                "must_have_skills": must_have_skills,  # Now just list of strings
                "nice_to_have_skills": nice_to_have_skills,  # Now just list of strings
                "created_at": datetime.utcnow().isoformat(),
                "total_screenings": 0,
                "total_candidates": 0,
                "status": "active"
            }
            
            if blob_url:
                job_data["blob_url"] = blob_url
            
            self.jobs_container.create_item(body=job_data)
            
            # Update user statistics
            await self.update_user_stats(user_id, increment_jobs=1)
            
            return job_id
        
        except Exception as e:
            raise Exception(f"Failed to create job description in database: {str(e)}")
        
    
    
    async def get_job_description(self, job_id: str, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Get job description by ID (user-specific)
        
        Args:
            job_id: Job ID
            user_id: User ID (partition key)
        
        Returns:
            Job description data or None
        """
        try:
            item = self.jobs_container.read_item(
                item=job_id,
                partition_key=user_id
            )
            return item
        
        except exceptions.CosmosResourceNotFoundError:
            return None
        except Exception as e:
            raise Exception(f"Failed to retrieve job description: {str(e)}")
    
    async def update_job_screening_count(self, job_id: str, user_id: str):
        """
        Increment the screening count for a job
        
        Args:
            job_id: Job ID
            user_id: User ID
        """
        try:
            job_data = await self.get_job_description(job_id, user_id)
            if job_data:
                job_data["total_screenings"] = job_data.get("total_screenings", 0) + 1
                job_data["total_candidates"] = job_data.get("total_candidates", 0) + 1
                job_data["last_screening_at"] = datetime.utcnow().isoformat()
                self.jobs_container.upsert_item(body=job_data)
                
                # Update user statistics
                await self.update_user_stats(user_id, increment_screenings=1)
        
        except Exception as e:
            print(f"Failed to update screening count: {str(e)}")

    async def check_duplicate_screening_name(
        self,
        user_id: str,
        screening_name: str
    ) -> bool:
        """
        Check if screening_name already exists for this user
        
        Args:
            user_id: User ID
            screening_name: Screening name to check
        
        Returns:
            True if duplicate exists, False otherwise
        """
        try:
            query = """
            SELECT VALUE COUNT(1)
            FROM c
            WHERE c.user_id = @user_id 
            AND c.screening_name = @screening_name
            """
            
            parameters = [
                {"name": "@user_id", "value": user_id},
                {"name": "@screening_name", "value": screening_name}
            ]
            
            result = list(self.jobs_container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True
            ))
            
            count = result[0] if result else 0
            return count > 0
            
        except Exception as e:
            print(f"Error checking duplicate screening name: {str(e)}")
            return False  # If error, allow creation (fail open)

    async def create_screening_job(
        self,
        screening_job_id: str,
        job_id: str,
        user_id: str,
        total_resumes: int
    ) -> str:
        """
        Create a screening job entry to track batch progress
        
        Args:
            screening_job_id: Unique screening job ID
            job_id: Job description ID
            user_id: User ID
            total_resumes: Total number of resumes to process
        
        Returns:
            Screening job ID
        """
        try:
            # Create screening_jobs container if not exists
            if not hasattr(self, 'screening_jobs_container'):
                self.screening_jobs_container = self.database.create_container_if_not_exists(
                    id=settings.COSMOS_DB_CONTAINER_SCREENING_JOBS,
                    partition_key=PartitionKey(path="/user_id"),
                    offer_throughput=400
                )
            
            screening_job_data = {
                "id": screening_job_id,
                "screening_job_id": screening_job_id,
                "job_id": job_id,
                "user_id": user_id,
                "total_resumes": total_resumes,
                "processed_resumes": 0,
                "successful_resumes": 0,
                "failed_resumes": 0,
                "status": "processing",  # processing, completed, failed
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat(),
                "resume_statuses": []  # List of {filename, status, processed_at}
            }
            
            self.screening_jobs_container.create_item(body=screening_job_data)
            return screening_job_id
        
        except Exception as e:
            raise Exception(f"Failed to create screening job: {str(e)}")
        
    async def get_screening_job(self, screening_job_id: str) -> Optional[Dict[str, Any]]:
        """Get screening job by ID"""
        try:
            if not hasattr(self, 'screening_jobs_container'):
                return None
            
            query = "SELECT * FROM c WHERE c.screening_job_id = @screening_job_id"
            parameters = [{"name": "@screening_job_id", "value": screening_job_id}]
            
            items = list(self.screening_jobs_container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True
            ))
            
            return items[0] if items else None
        
        except Exception as e:
            print(f"Error getting screening job: {str(e)}")
            return None
        
    async def get_screening_job_status(
        self,
        screening_job_id: str,
        user_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get current status of a screening job (for polling)
        
        Args:
            screening_job_id: Screening job ID
            user_id: User ID (for authorization)
        
        Returns:
            Status dictionary with progress information
        """
        try:
            screening_job = await self.get_screening_job(screening_job_id)
            
            if not screening_job or screening_job.get("user_id") != user_id:
                return None
            
            # Get completed screening results
            completed_screenings = await self.get_screening_results(
                job_id=screening_job["job_id"]
            )
            
            # Filter only screenings from this batch (if needed)
            # For now, return all recent ones
            
            return {
                "screening_job_id": screening_job_id,
                "status": screening_job["status"],
                "total_resumes": screening_job["total_resumes"],
                "processed_resumes": screening_job["processed_resumes"],
                "successful_resumes": screening_job["successful_resumes"],
                "failed_resumes": screening_job["failed_resumes"],
                "progress_percentage": screening_job.get("progress_percentage", 0),
                "created_at": screening_job["created_at"],
                "updated_at": screening_job["updated_at"],
                "completed_results": completed_screenings[-screening_job["processed_resumes"]:] if completed_screenings else []
            }
        
        except Exception as e:
            print(f"Error getting screening job status: {str(e)}")
            return None
        
    async def update_screening_job_progress(
        self,
        screening_job_id: str,
        resume_filename: str,
        status: str,  # "success" or "failed"
        screening_id: Optional[str] = None
    ) -> bool:
        """
        Update progress of a screening job after processing one resume
        
        Args:
            screening_job_id: Screening job ID
            resume_filename: Name of the processed resume
            status: "success" or "failed"
            screening_id: Optional screening result ID
        
        Returns:
            True if updated successfully
        """
        try:
            if not hasattr(self, 'screening_jobs_container'):
                return False
            
            # Get current screening job
            screening_job = await self.get_screening_job(screening_job_id)
            if not screening_job:
                return False
            
            # Update counters
            screening_job["processed_resumes"] += 1
            
            if status == "success":
                screening_job["successful_resumes"] += 1
            else:
                screening_job["failed_resumes"] += 1
            
            # Add resume status
            screening_job["resume_statuses"].append({
                "filename": resume_filename,
                "status": status,
                "processed_at": datetime.utcnow().isoformat(),
                "screening_id": screening_id
            })
            
            # Update overall status
            if screening_job["processed_resumes"] >= screening_job["total_resumes"]:
                screening_job["status"] = "completed"
            
            screening_job["updated_at"] = datetime.utcnow().isoformat()
            
            # Calculate progress percentage
            screening_job["progress_percentage"] = int(
                (screening_job["processed_resumes"] / screening_job["total_resumes"]) * 100
            )
            
            # Update in database
            self.screening_jobs_container.upsert_item(body=screening_job)
            
            print(f" Progress: {screening_job['processed_resumes']}/{screening_job['total_resumes']} ({screening_job['progress_percentage']}%)")
            
            return True
        
        except Exception as e:
            print(f"Error updating screening job progress: {str(e)}")
            return False
    
    async def save_screening_result(
        self,
        job_id: str,
        user_id: str,
        candidate_report: Dict[str, Any]
    ) -> str:
        """
        Save screening result for a candidate
        
        Args:
            job_id: Job ID
            user_id: User ID
            candidate_report: Candidate screening report
        
        Returns:
            Screening result ID
        """
        try:
            screening_id = str(uuid.uuid4())
            
            screening_data = {
                "id": screening_id,
                "job_id": job_id,
                "user_id": user_id,
                "screening_id": screening_id,
                "candidate_name": candidate_report.get("candidate_name"),
                "resume_url": candidate_report.get("resume_url"),
                "fit_score": candidate_report.get("fit_score"),
                "interview_worthy": candidate_report.get("interview_worthy"),
                "screening_details": candidate_report,
                "screened_at": datetime.utcnow().isoformat(),
                "status": "completed"
            }
            
            self.screenings_container.create_item(body=screening_data)
            
            # Update job screening count
            await self.update_job_screening_count(job_id, user_id)
            
            return screening_id
        
        except Exception as e:
            raise Exception(f"Failed to save screening result: {str(e)}")
    
    async def get_screening_results(
        self,
        job_id: str
    ) -> list:
        """
        Get all screening results for a job
         FIXED: Adds SAS token to resume URLs
        
        Args:
            job_id: Job ID
        
        Returns:
            List of screening results with working resume URLs
        """
        try:
            if not hasattr(self, 'screenings_container'):
                return []
            
            query = "SELECT * FROM c WHERE c.job_id = @job_id ORDER BY c.screened_at DESC"
            parameters = [{"name": "@job_id", "value": job_id}]
            
            results = list(self.screenings_container.query_items(
                query=query,
                parameters=parameters,
                partition_key=job_id
            ))
            
            #  Add SAS tokens to resume URLs
            from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
            from datetime import datetime, timedelta
            
            try:
                blob_service_client = BlobServiceClient.from_connection_string(
                    settings.AZURE_STORAGE_CONNECTION_STRING
                )
                
                # Extract account name and key from connection string
                conn_parts = dict(item.split('=', 1) for item in settings.AZURE_STORAGE_CONNECTION_STRING.split(';') if '=' in item)
                account_name = conn_parts.get('AccountName')
                account_key = conn_parts.get('AccountKey')
                
                for result in results:
                    resume_url = result.get("resume_url")
                    if resume_url and account_name and account_key:
                        # Parse blob name from URL
                        # URL format: https://{account}.blob.core.windows.net/{container}/{blob_path}
                        try:
                            url_parts = resume_url.split(f"{account_name}.blob.core.windows.net/")
                            if len(url_parts) == 2:
                                path_parts = url_parts[1].split('/', 1)
                                container_name = path_parts[0]
                                blob_name = path_parts[1] if len(path_parts) > 1 else ""
                                
                                # Generate SAS token (valid for 30 days)
                                sas_token = generate_blob_sas(
                                    account_name=account_name,
                                    container_name=container_name,
                                    blob_name=blob_name,
                                    account_key=account_key,
                                    permission=BlobSasPermissions(read=True),
                                    expiry=datetime.utcnow() + timedelta(days=30)
                                )
                                
                                # Add SAS token to URL
                                result["resume_url"] = f"{resume_url}?{sas_token}"
                        except Exception as e:
                            print(f"  Could not add SAS token to URL: {str(e)}")
            
            except Exception as e:
                print(f"  Could not generate SAS tokens: {str(e)}")
            
            return results
    
        except Exception as e:
            print(f"Error getting screening results: {str(e)}")
            return []
    
    async def get_screening_by_id(
        self,
        screening_id: str,
        job_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get specific screening result
        
        Args:
            screening_id: Screening ID
            job_id: Job ID (partition key)
        
        Returns:
            Screening result or None
        """
        try:
            item = self.screenings_container.read_item(
                item=screening_id,
                partition_key=job_id
            )
            return item
        
        except exceptions.CosmosResourceNotFoundError:
            return None
        except Exception as e:
            raise Exception(f"Failed to retrieve screening result: {str(e)}")
    
    async def get_all_jobs_with_counts(self, user_id: str) -> List[Dict[str, Any]]:
        """
        Get all job descriptions for a specific user with screening counts
        
        Args:
            user_id: User ID
        
        Returns:
            List of all jobs for the user with counts
        """
        try:
            query = "SELECT * FROM c WHERE c.user_id = @user_id ORDER BY c.created_at DESC"
            parameters = [{"name": "@user_id", "value": user_id}]
            
            items = list(self.jobs_container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=False,
                partition_key=user_id
            ))
            
            # Enrich each job with screening counts
            for job in items:
                job_id = job.get("job_id")
                
                # Get count of screenings for this job
                screening_count_query = "SELECT VALUE COUNT(1) FROM c WHERE c.job_id = @job_id"
                screening_count_params = [{"name": "@job_id", "value": job_id}]
                
                try:
                    count_result = list(self.screenings_container.query_items(
                        query=screening_count_query,
                        parameters=screening_count_params,
                        enable_cross_partition_query=False,
                        partition_key=job_id
                    ))
                    
                    actual_count = count_result[0] if count_result else 0
                    
                    job["total_screenings"] = actual_count
                    job["total_candidates"] = actual_count
                    
                except Exception as e:
                    print(f"Error getting count for job {job_id}: {str(e)}")
                    job["total_screenings"] = job.get("total_screenings", 0)
                    job["total_candidates"] = job.get("total_candidates", 0)
            
            return items
        
        except Exception as e:
            raise Exception(f"Failed to retrieve all jobs with counts: {str(e)}")
        
    # Add this method to the CosmosDBService class

    async def get_jobs_with_filters(
        self,
        user_id: str,
        search: Optional[str] = None,
        page_number: int = 1,
        page_size: int = 10,
        sort_by: str = "recent"
    ) -> Dict[str, Any]:
        """
        Get jobs for a user with advanced filtering, pagination, and sorting
        
        Args:
            user_id: User ID
            search: Search term for screening_name or job_description_text
            page_number: Page number (starts from 1)
            page_size: Number of items per page
            sort_by: Sort order - 'recent', 'oldest', 'week', 'month', 'name'
        
        Returns:
            Dictionary with jobs, pagination metadata
        """
        try:
            from datetime import datetime, timedelta
            
            # Build query conditions
            conditions = ["c.user_id = @user_id"]
            parameters = [{"name": "@user_id", "value": user_id}]
            
            # Add search filter
            if search:
                conditions.append("(CONTAINS(LOWER(c.screening_name), LOWER(@search)) OR CONTAINS(LOWER(c.job_description_text), LOWER(@search)))")
                parameters.append({"name": "@search", "value": search})
            
            # Add date filters for 'week' or 'month'
            if sort_by == "week":
                week_ago = (datetime.utcnow() - timedelta(days=7)).isoformat()
                conditions.append("c.created_at >= @date_filter")
                parameters.append({"name": "@date_filter", "value": week_ago})
            elif sort_by == "month":
                month_ago = (datetime.utcnow() - timedelta(days=30)).isoformat()
                conditions.append("c.created_at >= @date_filter")
                parameters.append({"name": "@date_filter", "value": month_ago})
            
            # Build WHERE clause
            where_clause = " AND ".join(conditions)
            
            # Determine ORDER BY clause
            if sort_by == "oldest":
                order_by = "ORDER BY c.created_at ASC"
            elif sort_by == "name":
                order_by = "ORDER BY c.screening_name ASC"
            else:  # 'recent', 'week', 'month' all sort by recent
                order_by = "ORDER BY c.created_at DESC"
            
            # Count total matching jobs
            count_query = f"SELECT VALUE COUNT(1) FROM c WHERE {where_clause}"
            count_result = list(self.jobs_container.query_items(
                query=count_query,
                parameters=parameters,
                enable_cross_partition_query=False,
                partition_key=user_id
            ))
            total_jobs = count_result[0] if count_result else 0
            
            # Calculate pagination
            total_pages = (total_jobs + page_size - 1) // page_size  # Ceiling division
            offset = (page_number - 1) * page_size
            
            # Get paginated jobs
            query = f"""
            SELECT * FROM c 
            WHERE {where_clause}
            {order_by}
            OFFSET {offset} LIMIT {page_size}
            """
            
            items = list(self.jobs_container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=False,
                partition_key=user_id
            ))
            
            # Enrich each job with screening counts
            for job in items:
                job_id = job.get("job_id")
                
                # Get count of screenings for this job
                screening_count_query = "SELECT VALUE COUNT(1) FROM c WHERE c.job_id = @job_id"
                screening_count_params = [{"name": "@job_id", "value": job_id}]
                
                try:
                    count_result = list(self.screenings_container.query_items(
                        query=screening_count_query,
                        parameters=screening_count_params,
                        enable_cross_partition_query=False,
                        partition_key=job_id
                    ))
                    
                    actual_count = count_result[0] if count_result else 0
                    job["total_screenings"] = actual_count
                    job["total_candidates"] = actual_count
                    
                except Exception as e:
                    print(f"Error getting count for job {job_id}: {str(e)}")
                    job["total_screenings"] = job.get("total_screenings", 0)
                    job["total_candidates"] = job.get("total_candidates", 0)
            
            return {
                "total_jobs": total_jobs,
                "total_pages": total_pages,
                "current_page": page_number,
                "page_size": page_size,
                "jobs": items
            }
        
        except Exception as e:
            raise Exception(f"Failed to get jobs with filters: {str(e)}")
    
    async def get_statistics(self, job_id: str) -> Dict[str, Any]:
        """
        Get statistics for a job's screening results
        
        Args:
            job_id: Job ID
        
        Returns:
            Statistics dictionary
        """
        try:
            screenings = await self.get_screening_results(job_id)
            
            if not screenings:
                return {
                    "total_screened": 0,
                    "average_fit_score": 0,
                    "interview_worthy_count": 0,
                    "interview_worthy_percentage": 0
                }
            
            total = len(screenings)
            fit_scores = [s["fit_score"]["score"] for s in screenings]
            interview_worthy = sum(1 for s in screenings if s["interview_worthy"])
            
            return {
                "total_screened": total,
                "average_fit_score": sum(fit_scores) / total if fit_scores else 0,
                "interview_worthy_count": interview_worthy,
                "interview_worthy_percentage": (interview_worthy / total * 100) if total > 0 else 0,
                "highest_fit_score": max(fit_scores) if fit_scores else 0,
                "lowest_fit_score": min(fit_scores) if fit_scores else 0
            }
        
        except Exception as e:
            raise Exception(f"Failed to calculate statistics: {str(e)}")
    
    async def delete_job_and_screenings(self, job_id: str, user_id: str) -> bool:
        """
        Delete job and all associated screening results
        
        Args:
            job_id: Job ID
            user_id: User ID
        
        Returns:
            True if successful
        """
        try:
            # Delete all screening results
            screenings = await self.get_screening_results(job_id)
            for screening in screenings:
                self.screenings_container.delete_item(
                    item=screening["id"],
                    partition_key=job_id
                )
            
            # Delete job
            self.jobs_container.delete_item(
                item=job_id,
                partition_key=user_id
            )
            
            return True
        
        except Exception as e:
            print(f"Failed to delete job and screenings: {str(e)}")
            return False
        
    # Add this method to the CosmosDBService class

    async def get_user_statistics(self, user_id: str) -> Dict[str, Any]:
        """
        Get comprehensive statistics for a user
        
        Args:
            user_id: User ID
        
        Returns:
            Dictionary with user statistics
        """
        try:
            # Get all jobs for user
            jobs_query = "SELECT * FROM c WHERE c.user_id = @user_id"
            jobs_params = [{"name": "@user_id", "value": user_id}]
            
            jobs = list(self.jobs_container.query_items(
                query=jobs_query,
                parameters=jobs_params,
                enable_cross_partition_query=False,
                partition_key=user_id
            ))
            
            total_job_descriptions = len(jobs)
            total_resumes_screened = 0
            jobs_with_screenings = 0
            jobs_summary = []
            
            # Get screening counts for each job
            for job in jobs:
                job_id = job.get("job_id")
                
                # Get count of screenings for this job
                screening_count_query = "SELECT VALUE COUNT(1) FROM c WHERE c.job_id = @job_id"
                screening_count_params = [{"name": "@job_id", "value": job_id}]
                
                try:
                    count_result = list(self.screenings_container.query_items(
                        query=screening_count_query,
                        parameters=screening_count_params,
                        enable_cross_partition_query=False,
                        partition_key=job_id
                    ))
                    
                    screening_count = count_result[0] if count_result else 0
                    total_resumes_screened += screening_count
                    
                    if screening_count > 0:
                        jobs_with_screenings += 1
                    
                    jobs_summary.append({
                        "job_id": job_id,
                        "screening_name": job.get("screening_name"),
                        "created_at": job.get("created_at"),
                        "total_screenings": screening_count,
                        "must_have_skills": job.get("must_have_skills", []),
                        "nice_to_have_skills": job.get("nice_to_have_skills", [])
                    })
                    
                except Exception as e:
                    print(f"Error getting screenings for job {job_id}: {str(e)}")
                    jobs_summary.append({
                        "job_id": job_id,
                        "screening_name": job.get("screening_name"),
                        "created_at": job.get("created_at"),
                        "total_screenings": 0,
                        "must_have_skills": job.get("must_have_skills", []),
                        "nice_to_have_skills": job.get("nice_to_have_skills", [])
                    })
            
            return {
                "user_id": user_id,
                "total_job_descriptions": total_job_descriptions,
                "total_resumes_screened": total_resumes_screened,
                "total_jobs_with_screenings": jobs_with_screenings,
                "jobs_summary": jobs_summary
            }
        
        except Exception as e:
            raise Exception(f"Failed to get user statistics: {str(e)}")
        
    # Add these methods to the existing CosmosDBService class

    async def get_screening_job_by_job_id(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get screening job by job_id
        
        Args:
            job_id: Job description ID
        
        Returns:
            Screening job data or None
        """
        try:
            # Initialize container if needed
            if not hasattr(self, 'screening_jobs_container'):
                try:
                    self.screening_jobs_container = self.database.get_container_client(
                        settings.COSMOS_DB_CONTAINER_SCREENING_JOBS
                    )
                except:
                    # Container doesn't exist yet
                    return None
            
            # Try to read item directly using job_id as both id and partition key
            try:
                item = self.screening_jobs_container.read_item(
                    item=job_id,
                    partition_key=job_id
                )
                return item
            except exceptions.CosmosResourceNotFoundError:
                # Item doesn't exist
                return None
        
        except Exception as e:
            print(f"Error getting screening job: {str(e)}")
            return None


    async def initialize_screening_job_for_job(
        self,
        job_id: str,
        user_id: str
    ) -> bool:
        """
        Initialize screening job tracking for a job_id
        FIXED: Ensure container exists and handle conflicts properly
        """
        try:
            # Ensure container exists
            if not hasattr(self, 'screening_jobs_container'):
                try:
                    # Try to get existing container first
                    self.screening_jobs_container = self.database.get_container_client(
                        settings.COSMOS_DB_CONTAINER_SCREENING_JOBS
                    )
                except:
                    # Container doesn't exist, create it
                    self.screening_jobs_container = self.database.create_container_if_not_exists(
                        id=settings.COSMOS_DB_CONTAINER_SCREENING_JOBS,
                        partition_key=PartitionKey(path="/job_id")
                    )
            
            # Check if screening job already exists
            existing = await self.get_screening_job_by_job_id(job_id)
            if existing:
                print(f"        Screening job already exists for job_id: {job_id}")
                return True
            
            # Create new screening job
            screening_job_data = {
                "id": job_id,
                "job_id": job_id,
                "user_id": user_id,
                "total_resumes": 0,  # Will be calculated from blob
                "processed_resumes": 0,
                "successful_resumes": 0,
                "failed_resumes": 0,
                "status": "processing",
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat(),
                "resume_statuses": []
            }
            
            try:
                self.screening_jobs_container.create_item(body=screening_job_data)
                print(f"       Created screening job tracker for job_id: {job_id}")
                return True
            except exceptions.CosmosResourceExistsError:
                # Another worker already created it (race condition)
                print(f"        Screening job created by another worker")
                return True
        
        except Exception as e:
            print(f" Error initializing screening job: {str(e)}")
            import traceback
            traceback.print_exc()
            return True  # Don't fail the whole process


    async def update_screening_job_progress_by_job_id(
        self,
        job_id: str,
        resume_filename: str,
        status: str,
        screening_id: Optional[str] = None
    ) -> bool:
        """
        Update progress after processing one resume
         UPDATED: Updates both current_batch and all_time counters
        """
        try:
            # Ensure container exists
            if not hasattr(self, 'screening_jobs_container'):
                try:
                    self.screening_jobs_container = self.database.get_container_client(
                        settings.COSMOS_DB_CONTAINER_SCREENING_JOBS
                    )
                except:
                    print(f" Screening jobs container doesn't exist")
                    return False
            
            # Get current screening job
            screening_job = await self.get_screening_job_by_job_id(job_id)
            
            if not screening_job:
                print(f"  No tracker found")
                return False
            
            #  Update CURRENT BATCH counters
            screening_job["current_batch_processed"] = screening_job.get("current_batch_processed", 0) + 1
            
            if status == "success":
                screening_job["current_batch_successful"] = screening_job.get("current_batch_successful", 0) + 1
            else:
                screening_job["current_batch_failed"] = screening_job.get("current_batch_failed", 0) + 1
            
            #  Update ALL-TIME counters
            screening_job["processed_resumes"] = screening_job.get("processed_resumes", 0) + 1
            
            if status == "success":
                screening_job["successful_resumes"] = screening_job.get("successful_resumes", 0) + 1
            else:
                screening_job["failed_resumes"] = screening_job.get("failed_resumes", 0) + 1
            
            # Add resume status
            if "resume_statuses" not in screening_job:
                screening_job["resume_statuses"] = []
            
            screening_job["resume_statuses"].append({
                "filename": resume_filename,
                "status": status,
                "processed_at": datetime.utcnow().isoformat(),
                "screening_id": screening_id
            })
            
            # Update timestamp
            screening_job["updated_at"] = datetime.utcnow().isoformat()
            
            # Save to database
            self.screening_jobs_container.upsert_item(body=screening_job)
            
            current_batch_total = screening_job.get("current_batch_total", 0)
            current_batch_processed = screening_job.get("current_batch_processed", 0)
            
            print(f" Updated progress:")
            print(f"   Current batch: {current_batch_processed}/{current_batch_total}")
            print(f"   All-time: {screening_job['processed_resumes']}")
            
            return True
        
        except Exception as e:
            print(f" Error updating progress: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    # Add this method to CosmosDBService class

    async def is_resume_already_processed(self, job_id: str, resume_filename: str) -> bool:
        """
        Check if resume has already been processed (prevents duplicates)
        
        Args:
            job_id: Job ID
            resume_filename: Resume filename
        
        Returns:
            True if already processed, False otherwise
        """
        try:
            if not hasattr(self, 'screenings_container'):
                return False
            
            # Query to check if this resume was already processed
            query = "SELECT VALUE COUNT(1) FROM c WHERE c.job_id = @job_id AND c.resume_filename = @filename"
            parameters = [
                {"name": "@job_id", "value": job_id},
                {"name": "@filename", "value": resume_filename}
            ]
            
            result = list(self.screenings_container.query_items(
                query=query,
                parameters=parameters,
                partition_key=job_id
            ))
            
            count = result[0] if result else 0
            return count > 0
        
        except Exception as e:
            print(f"Error checking duplicate: {str(e)}")
            return False


    async def get_total_resumes_in_blob(self, job_id: str) -> int:
        """
        Count total resumes currently in blob storage for a job
         ENHANCED: Better error handling and debugging
        
        Args:
            job_id: Job ID
        
        Returns:
            Total count of resume files in blob storage
        """
        try:
            from azure.storage.blob import BlobServiceClient
            
            print(f" Counting blobs for job_id: {job_id}")
            print(f"   Container: {settings.AZURE_STORAGE_CONTAINER_RESUMES}")
            print(f"   Blob prefix: {job_id}/")
            
            # Initialize blob client
            blob_service_client = BlobServiceClient.from_connection_string(
                settings.AZURE_STORAGE_CONNECTION_STRING
            )
            
            container_client = blob_service_client.get_container_client(
                settings.AZURE_STORAGE_CONTAINER_RESUMES
            )
            
            # List all blobs with job_id prefix
            blob_prefix = f"{job_id}/"
            print(f"   Listing blobs with prefix: {blob_prefix}")
            
            blobs = container_client.list_blobs(name_starts_with=blob_prefix)
            
            # Count blobs (excluding folders)
            count = 0
            blob_names = []
            
            for blob in blobs:
                print(f"   Found blob: {blob.name}")
                # Skip if it's a folder (ends with /)
                if not blob.name.endswith('/'):
                    count += 1
                    blob_names.append(blob.name)
            
            print(f" Total files in blob storage for job {job_id}: {count}")
            if blob_names:
                print(f"   Files: {blob_names}")
            else:
                print(f"    No files found! Check if files were uploaded.")
            
            return count
        
        except Exception as e:
            print(f" Error counting blobs: {str(e)}")
            print(f"   Connection string exists: {bool(settings.AZURE_STORAGE_CONNECTION_STRING)}")
            print(f"   Container name: {settings.AZURE_STORAGE_CONTAINER_RESUMES}")
            import traceback
            traceback.print_exc()
            return 0


        
    async def get_comprehensive_screening_status(
        self,
        job_id: str,
        user_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get comprehensive screening status
        UPDATED: Uses current_batch_total from tracker
        """
        try:
            # 1. Get full job details
            job_data = await self.get_job_description(job_id, user_id)
            if not job_data:
                print(f" Job not found: {job_id} for user: {user_id}")
                return None
            
            print(f" Found job: {job_data.get('screening_name')}")
            
            # 2. Get ALL screening results (all time)
            all_screenings = await self.get_screening_results(job_id)
            total_candidates_screened = len(all_screenings)
            print(f" Total candidates screened (all time): {total_candidates_screened}")
            
            # 3. Get screening job tracker
            screening_job = await self.get_screening_job_by_job_id(job_id)
            
            # 4. Calculate current batch status
            if screening_job:
                current_batch_total = screening_job.get("current_batch_total", 0)
                current_batch_processed = screening_job.get("current_batch_processed", 0)
                current_batch_successful = screening_job.get("current_batch_successful", 0)
                current_batch_failed = screening_job.get("current_batch_failed", 0)
                
                print(f" Found tracker:")
                print(f"   Current batch total: {current_batch_total}")
                print(f"   Current batch processed: {current_batch_processed}")
                print(f"   Current batch successful: {current_batch_successful}")
                print(f"   Current batch failed: {current_batch_failed}")
                
                if current_batch_total > 0:
                    remaining = max(0, current_batch_total - current_batch_processed)
                    
                    #  Calculate progress percentage correctly
                    progress_percentage = int((current_batch_processed / current_batch_total) * 100)
                    progress_percentage = min(100, progress_percentage)  # Cap at 100
                    
                    # Determine status
                    if remaining == 0:
                        status = "completed"
                        progress_percentage = 100
                        print(f"   Status: COMPLETED")
                    elif current_batch_processed > 0:
                        status = "processing"
                        print(f"   Status: PROCESSING ({current_batch_processed}/{current_batch_total})")
                    else:
                        status = "pending"
                        print(f"   Status: PENDING")
                    
                    current_batch = {
                        "total_uploaded_in_queue": current_batch_total,
                        "processed": current_batch_processed,
                        "successful": current_batch_successful,
                        "failed": current_batch_failed,
                        "remaining": remaining,
                        "status": status,
                        "progress_percentage": progress_percentage,
                        "batch_start_time": screening_job.get("batch_start_time")
                    }
                else:
                    # No active batch
                    current_batch = {
                        "total_uploaded_in_queue": 0,
                        "processed": 0,
                        "successful": 0,
                        "failed": 0,
                        "remaining": 0,
                        "status": "no_resumes_in_queue",
                        "progress_percentage": 0
                    }
            else:
                # No tracker = no files uploaded
                print(f"  No tracker found")
                current_batch = {
                    "total_uploaded_in_queue": 0,
                    "processed": 0,
                    "successful": 0,
                    "failed": 0,
                    "remaining": 0,
                    "status": "no_resumes_in_queue",
                    "progress_percentage": 0
                }
            
            # 5. Return complete response
            return {
                "job_id": job_data.get("job_id"),
                "screening_name": job_data.get("screening_name"),
                "job_description_text": job_data.get("job_description_text"),
                "filename": job_data.get("filename"),
                "must_have_skills": job_data.get("must_have_skills", []),
                "nice_to_have_skills": job_data.get("nice_to_have_skills", []),
                "created_at": job_data.get("created_at"),
                "blob_url": job_data.get("blob_url"),
                "total_candidates_screened": total_candidates_screened,
                "current_batch": current_batch,
                "screening_results": all_screenings
            }
        
        except Exception as e:
            print(f" Error in get_comprehensive_screening_status: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    async def initialize_or_increment_batch_total(
        self,
        job_id: str,
        user_id: str
    ) -> bool:
        """
        Initialize tracker and detect new batch
         FIXED: Correctly detects new files vs old files
        """
        try:
            from azure.storage.blob import BlobServiceClient
            
            print(f"\n{'='*60}")
            print(f" BATCH DETECTION FOR JOB: {job_id}")
            print(f"{'='*60}")
            
            # Ensure container exists
            if not hasattr(self, 'screening_jobs_container'):
                try:
                    self.screening_jobs_container = self.database.get_container_client(
                        settings.COSMOS_DB_CONTAINER_SCREENING_JOBS
                    )
                except:
                    self.screening_jobs_container = self.database.create_container_if_not_exists(
                        id=settings.COSMOS_DB_CONTAINER_SCREENING_JOBS,
                        partition_key=PartitionKey(path="/job_id")
                    )
            
            # Get files currently in blob
            blob_service_client = BlobServiceClient.from_connection_string(
                settings.AZURE_STORAGE_CONNECTION_STRING
            )
            
            container_client = blob_service_client.get_container_client(
                settings.AZURE_STORAGE_CONTAINER_RESUMES
            )
            
            blob_prefix = f"{job_id}/"
            files_in_blob = set()
            
            for blob in container_client.list_blobs(name_starts_with=blob_prefix):
                if not blob.name.endswith('/'):
                    filename = blob.name.replace(blob_prefix, "")
                    files_in_blob.add(filename)
            
            print(f" Files in blob storage: {len(files_in_blob)}")
            for f in list(files_in_blob)[:5]:
                print(f"   - {f}")
            
            # Get existing tracker
            screening_job = await self.get_screening_job_by_job_id(job_id)
            
            if not screening_job:
                #  NO TRACKER = FIRST BATCH EVER
                print(f"\n NO TRACKER - Creating first batch")
                print(f"   New batch size: {len(files_in_blob)}")
                
                screening_job_data = {
                    "id": job_id,
                    "job_id": job_id,
                    "user_id": user_id,
                    "current_batch_total": len(files_in_blob),
                    "current_batch_processed": 0,
                    "current_batch_successful": 0,
                    "current_batch_failed": 0,
                    "current_batch_files": list(files_in_blob),  #  Store filenames
                    "processed_resumes": 0,
                    "successful_resumes": 0,
                    "failed_resumes": 0,
                    "status": "processing",
                    "created_at": datetime.utcnow().isoformat(),
                    "updated_at": datetime.utcnow().isoformat(),
                    "batch_start_time": datetime.utcnow().isoformat(),
                    "resume_statuses": []
                }
                
                try:
                    self.screening_jobs_container.create_item(body=screening_job_data)
                    print(f"    Created tracker")
                except exceptions.CosmosResourceExistsError:
                    print(f"    Created by another message")
                
                return True
            
            # Tracker exists - check if new batch needed
            current_batch_files = set(screening_job.get("current_batch_files", []))
            current_batch_processed = screening_job.get("current_batch_processed", 0)
            current_batch_total = screening_job.get("current_batch_total", 0)
            
            print(f"\n TRACKER STATUS:")
            print(f"   Current batch total: {current_batch_total}")
            print(f"   Current batch processed: {current_batch_processed}")
            print(f"   Current batch files tracked: {len(current_batch_files)}")
            
            #  DETECT NEW FILES
            new_files = files_in_blob - current_batch_files
            
            print(f"\n COMPARISON:")
            print(f"   Files in blob: {len(files_in_blob)}")
            print(f"   Files in tracker: {len(current_batch_files)}")
            print(f"   NEW files detected: {len(new_files)}")
            
            if new_files:
                for f in list(new_files)[:5]:
                    print(f"      - {f}")
            
            # Check if previous batch completed AND new files exist
            batch_completed = (current_batch_processed >= current_batch_total) and current_batch_total > 0
            
            if batch_completed and new_files:
                #  NEW BATCH DETECTED
                print(f"\n NEW BATCH DETECTED!")
                print(f"   Previous batch: {current_batch_total} files (completed)")
                print(f"   New batch: {len(new_files)} files")
                
                # Reset for new batch
                screening_job["current_batch_total"] = len(new_files)
                screening_job["current_batch_processed"] = 0
                screening_job["current_batch_successful"] = 0
                screening_job["current_batch_failed"] = 0
                screening_job["current_batch_files"] = list(files_in_blob)  # Update file list
                screening_job["batch_start_time"] = datetime.utcnow().isoformat()
                screening_job["updated_at"] = datetime.utcnow().isoformat()
                
                self.screening_jobs_container.upsert_item(body=screening_job)
                print(f"    Reset tracker for new batch")
            
            elif not batch_completed and new_files:
                #  FILES ADDED TO ONGOING BATCH
                print(f"\n  WARNING: New files added while batch in progress")
                print(f"   Current batch not complete: {current_batch_processed}/{current_batch_total}")
                print(f"   Adding {len(new_files)} new files to batch")
                
                # Add new files to current batch
                screening_job["current_batch_total"] = current_batch_total + len(new_files)
                screening_job["current_batch_files"] = list(files_in_blob)
                screening_job["updated_at"] = datetime.utcnow().isoformat()
                
                self.screening_jobs_container.upsert_item(body=screening_job)
                print(f"    Updated batch total to {screening_job['current_batch_total']}")
            
            print(f"{'='*60}\n")
            return True
        
        except Exception as e:
            print(f" Error: {str(e)}")
            import traceback
            traceback.print_exc()
            return True
        
    async def reset_screening_job_for_new_batch(
        self,
        job_id: str
    ) -> bool:
        """
        Reset screening job tracker for a new batch
        Call this when starting a completely new upload session
        
         OPTIONAL: Only needed if you want to clear tracker between batches
        """
        try:
            screening_job = await self.get_screening_job_by_job_id(job_id)
            
            if screening_job:
                # Check if previous batch was completed
                if screening_job.get("status") == "completed":
                    # Delete old tracker to start fresh
                    self.screening_jobs_container.delete_item(
                        item=job_id,
                        partition_key=job_id
                    )
                    print(f" Deleted old completed tracker for new batch")
                    return True
            
            return False
        except Exception as e:
            print(f"Error resetting tracker: {str(e)}")
            return False

    async def should_reset_tracker_for_new_batch(
        self,
        job_id: str
    ) -> bool:
        """
        Check if we should reset the tracker for a new batch
        
        Returns True if:
        - Previous batch was completed (all files processed)
        - Some time has passed since last upload
        
        Returns:
            True if tracker should be reset, False otherwise
        """
        try:
            screening_job = await self.get_screening_job_by_job_id(job_id)
            
            if not screening_job:
                # No tracker exists = first batch
                return False
            
            total = screening_job.get("total_resumes", 0)
            processed = screening_job.get("processed_resumes", 0)
            
            # If previous batch was completed (all files processed)
            if total > 0 and processed >= total:
                print(f"        Previous batch was completed ({processed}/{total})")
                print(f"       Starting new batch - resetting tracker")
                return True
            
            return False
        
        except Exception as e:
            print(f"Error checking batch reset: {str(e)}")
            return False
        
    async def get_current_batch_info(self, job_id: str) -> Dict[str, Any]:
        """
        Get current batch information by comparing blob storage with processed files
         FIXED: Better filename comparison and detailed logging
        """
        try:
            from azure.storage.blob import BlobServiceClient
            
            print(f"\n{'='*60}")
            print(f" BATCH INFO ANALYSIS FOR JOB: {job_id}")
            print(f"{'='*60}")
            
            # 1. Get all files in blob storage
            blob_service_client = BlobServiceClient.from_connection_string(
                settings.AZURE_STORAGE_CONNECTION_STRING
            )
            
            container_client = blob_service_client.get_container_client(
                settings.AZURE_STORAGE_CONTAINER_RESUMES
            )
            
            blob_prefix = f"{job_id}/"
            files_in_blob = []
            
            print(f"\n STEP 1: Scanning blob storage...")
            print(f"   Container: {settings.AZURE_STORAGE_CONTAINER_RESUMES}")
            print(f"   Prefix: {blob_prefix}")
            
            for blob in container_client.list_blobs(name_starts_with=blob_prefix):
                # Skip folders
                if not blob.name.endswith('/'):
                    # Extract just the filename (remove job_id/ prefix)
                    filename = blob.name.replace(blob_prefix, "")
                    files_in_blob.append({
                        "filename": filename,
                        "full_path": blob.name,
                        "created": blob.creation_time
                    })
                    print(f"    Found: {filename}")
            
            print(f"\n    Total files in blob: {len(files_in_blob)}")
            
            # 2. Get processed files from tracker
            print(f"\n STEP 2: Checking processed files in tracker...")
            screening_job = await self.get_screening_job_by_job_id(job_id)
            
            processed_files = set()
            processed_files_list = []  # For detailed logging
            all_time_processed_count = 0
            all_time_successful_count = 0
            all_time_failed_count = 0
            
            if screening_job:
                resume_statuses = screening_job.get("resume_statuses", [])
                all_time_processed_count = screening_job.get("processed_resumes", 0)
                all_time_successful_count = screening_job.get("successful_resumes", 0)
                all_time_failed_count = screening_job.get("failed_resumes", 0)
                
                print(f"   Tracker stats:")
                print(f"   - Processed: {all_time_processed_count}")
                print(f"   - Successful: {all_time_successful_count}")
                print(f"   - Failed: {all_time_failed_count}")
                print(f"   - Resume statuses entries: {len(resume_statuses)}")
                
                for status in resume_statuses:
                    filename = status.get("filename")
                    if filename:
                        processed_files.add(filename)
                        processed_files_list.append({
                            "filename": filename,
                            "status": status.get("status"),
                            "processed_at": status.get("processed_at")
                        })
                        print(f"    Processed: {filename} ({status.get('status')})")
                
                print(f"\n    Total unique processed files: {len(processed_files)}")
            else:
                print(f"     No tracker found - all files are unprocessed")
            
            # 3. Find unprocessed files (current batch)
            print(f"\n STEP 3: Identifying unprocessed files...")
            unprocessed_files = []
            
            for blob_info in files_in_blob:
                blob_filename = blob_info["filename"]
                is_processed = blob_filename in processed_files
                
                print(f"   Checking: {blob_filename}")
                print(f"      → In processed list: {is_processed}")
                
                if not is_processed:
                    unprocessed_files.append(blob_info)
                    print(f"      →  UNPROCESSED (part of current batch)")
                else:
                    print(f"      →  Already processed (not in current batch)")
            
            print(f"\n    Unprocessed files (current batch): {len(unprocessed_files)}")
            
            # 4. Calculate current batch size
            # Current batch = unprocessed files only
            current_batch_size = len(unprocessed_files)
            current_batch_processed = 0
            current_batch_successful = 0
            current_batch_failed = 0
            
            print(f"\n STEP 4: Calculating batch metrics...")
            print(f"   Current batch size: {current_batch_size}")
            print(f"   Current batch processed: {current_batch_processed}")
            
            print(f"\n{'='*60}")
            print(f"SUMMARY:")
            print(f"{'='*60}")
            print(f"Files in blob storage: {len(files_in_blob)}")
            print(f"All-time processed: {all_time_processed_count}")
            print(f"Unprocessed (current batch): {len(unprocessed_files)}")
            print(f"{'='*60}\n")
            
            return {
                "total_in_blob": len(files_in_blob),
                "processed_files": list(processed_files),
                "processed_files_list": processed_files_list,
                "unprocessed_files": [f["filename"] for f in unprocessed_files],
                "current_batch_size": current_batch_size,
                "current_batch_processed": current_batch_processed,
                "current_batch_successful": current_batch_successful,
                "current_batch_failed": current_batch_failed,
                "all_time_processed": all_time_processed_count,
                "all_time_successful": all_time_successful_count,
                "all_time_failed": all_time_failed_count
            }
        
        except Exception as e:
            print(f" ERROR in get_current_batch_info: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                "total_in_blob": 0,
                "processed_files": [],
                "processed_files_list": [],
                "unprocessed_files": [],
                "current_batch_size": 0,
                "current_batch_processed": 0,
                "current_batch_successful": 0,
                "current_batch_failed": 0,
                "all_time_processed": 0,
                "all_time_successful": 0,
                "all_time_failed": 0
            }
        
    async def get_candidate_report(
        self,
        screening_id: str,
        job_id: str,
        user_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get detailed candidate report
         FIXED: Adds SAS token to resume URL
        """
        try:
            # Verify job belongs to user
            job_data = await self.get_job_description(job_id, user_id)
            if not job_data:
                return None
            
            # Get screening result
            if not hasattr(self, 'screenings_container'):
                return None
            
            try:
                screening = self.screenings_container.read_item(
                    item=screening_id,
                    partition_key=job_id
                )
                
                #  Add SAS token to resume URL
                from azure.storage.blob import generate_blob_sas, BlobSasPermissions
                from datetime import datetime, timedelta
                
                resume_url = screening.get("resume_url")
                if resume_url:
                    try:
                        # Extract connection string parts
                        conn_parts = dict(item.split('=', 1) for item in settings.AZURE_STORAGE_CONNECTION_STRING.split(';') if '=' in item)
                        account_name = conn_parts.get('AccountName')
                        account_key = conn_parts.get('AccountKey')
                        
                        if account_name and account_key:
                            # Parse URL
                            url_parts = resume_url.split(f"{account_name}.blob.core.windows.net/")
                            if len(url_parts) == 2:
                                path_parts = url_parts[1].split('/', 1)
                                container_name = path_parts[0]
                                blob_name = path_parts[1] if len(path_parts) > 1 else ""
                                
                                # Generate SAS token (30 days)
                                sas_token = generate_blob_sas(
                                    account_name=account_name,
                                    container_name=container_name,
                                    blob_name=blob_name,
                                    account_key=account_key,
                                    permission=BlobSasPermissions(read=True),
                                    expiry=datetime.utcnow() + timedelta(days=30)
                                )
                                
                                screening["resume_url"] = f"{resume_url}?{sas_token}"
                    
                    except Exception as e:
                        print(f"Could not add SAS token: {str(e)}")
                
                return screening
            
            except exceptions.CosmosResourceNotFoundError:
                return None
        
        except Exception as e:
            print(f"Error getting candidate report: {str(e)}")
            return None