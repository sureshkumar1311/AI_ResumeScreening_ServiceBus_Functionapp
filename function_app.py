import azure.functions as func
import logging
import json
import re
import os

app = func.FunctionApp()

# Get queue name from environment variable - hardcoded as fallback
QUEUE_NAME = os.getenv("AZURE_SERVICE_BUS_QUEUE_NAME", "resume-processing-queue")

def parse_event_grid_message(message_body):
    """Parse Event Grid message"""
    try:
        if isinstance(message_body, list):
            event = message_body[0]
        else:
            event = message_body
        
        blob_url = event.get("data", {}).get("url")
        if not blob_url:
            raise ValueError("No blob URL found in event data")
        
        subject = event.get("subject", "")
        match = re.search(r'/containers/([^/]+)/blobs/(.+)$', subject)
        
        if not match:
            raise ValueError(f"Cannot parse blob path from subject: {subject}")
        
        container_name = match.group(1)
        blob_path = match.group(2)
        
        path_parts = blob_path.split('/', 1)
        if len(path_parts) < 2:
            raise ValueError(f"Invalid blob path format: {blob_path}")
        
        job_id = path_parts[0]
        filename = path_parts[1]
        
        return {
            "job_id": job_id,
            "resume_blob_url": blob_url,
            "resume_filename": filename,
            "container_name": container_name
        }
    
    except Exception as e:
        logging.error(f"Error parsing Event Grid message: {str(e)}")
        raise


async def process_resume_message(message_data: dict):
    """Process a single resume screening message"""
    from azure_blob_service import AzureBlobService
    from document_parser import DocumentParser
    from ai_screening_service import AIScreeningService
    from cosmos_db_service import CosmosDBService
    
    blob_service = AzureBlobService()
    document_parser = DocumentParser()
    ai_service = AIScreeningService()
    cosmos_service = CosmosDBService()
    
    job_id = message_data["job_id"]
    resume_blob_url = message_data["resume_blob_url"]
    resume_filename = message_data["resume_filename"]
    
    logging.info(f"Processing Resume - Filename: {resume_filename}, Job ID: {job_id}")
    
    try:
        #  STEP 0.1: Verify blob URL matches job_id
        if f"/{job_id}/" not in resume_blob_url:
            error_msg = f"SECURITY ERROR: Blob URL doesn't match job_id"
            logging.error(error_msg)
            logging.error(f"   Expected job_id in URL: {job_id}")
            logging.error(f"   Actual blob URL: {resume_blob_url}")
            raise Exception(error_msg)
        
        logging.info(" Job ID verified in blob URL")
        
        # STEP 0.2: Check if already processed (DUPLICATE CHECK)
        is_duplicate = await cosmos_service.is_resume_already_processed(job_id, resume_filename)
        if is_duplicate:
            logging.warning(f"  Resume already processed - skipping duplicate")
            return
        
        # 1. Get job description
        logging.info("Step 1: Fetching job description...")
        
        query = "SELECT * FROM c WHERE c.job_id = @job_id"
        parameters = [{"name": "@job_id", "value": job_id}]
        
        jobs = list(cosmos_service.jobs_container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        ))
        
        if not jobs:
            raise Exception(f"Job not found: {job_id}")
        
        job_data = jobs[0]
        user_id = job_data["user_id"]
        
        #  VERIFY job_id in database matches message
        if job_data["job_id"] != job_id:
            raise Exception(f"Job ID mismatch: DB={job_data['job_id']}, Message={job_id}")
        
        logging.info(f" Job: {job_data.get('screening_name')}")
        logging.info(f" User: {user_id}")
        
        # 2. Update batch tracker
        logging.info("Step 2: Updating batch tracker...")
        await cosmos_service.initialize_or_increment_batch_total(job_id, user_id)
        
        # 3. Download resume from blob
        logging.info("Step 3: Downloading resume from blob storage...")
        resume_content = await blob_service.download_file(resume_blob_url)
        file_size_mb = len(resume_content) / (1024 * 1024)
        logging.info(f"Downloaded: {file_size_mb:.2f} MB")
        
        # 4. Parse resume
        logging.info("Step 4: Parsing resume text...")
        resume_text = await document_parser.parse_document(
            resume_content,
            resume_filename
        )
        text_length = len(resume_text)
        logging.info(f"Extracted: {text_length} characters")
        
        # 5. Perform AI screening
        logging.info("Step 5: Performing AI screening...")
        
        screening_result = await ai_service.screen_candidate(
            resume_text=resume_text,
            job_description=job_data["job_description_text"],
            must_have_skills=job_data["must_have_skills"],
            nice_to_have_skills=job_data["nice_to_have_skills"]
        )
        
        logging.info(f"AI screening completed - Fit Score: {screening_result['fit_score']['score']}%, Candidate: {screening_result['candidate_info']['name']}")
        
        # 6. Create candidate report
        logging.info("Step 6: Creating candidate report...")
        
        from models import CandidateReport
        
        # Validate AI summary
        ai_summary = screening_result["ai_summary"]
        if not ai_summary or len(ai_summary) < 3:
            ai_summary = [
                f"Candidate with {screening_result['candidate_info']['total_experience']} of experience",
                f"Position: {screening_result['candidate_info']['position']}",
                f"Skills match: {screening_result['skills_analysis']['must_have_matched']}/{screening_result['skills_analysis']['must_have_total']}"
            ]
        
        # Validate career gap
        professional_summary = screening_result["professional_summary"].copy()
        career_gap = professional_summary.get("career_gap")
        if career_gap and (not career_gap.get("duration") or not isinstance(career_gap.get("duration"), str)):
            professional_summary["career_gap"] = None
        
        candidate_report = CandidateReport(
            candidate_name=screening_result["candidate_info"]["name"],
            email=screening_result["candidate_info"].get("email"),
            phone=screening_result["candidate_info"].get("phone"),
            position=screening_result["candidate_info"]["position"],
            location=screening_result["candidate_info"]["location"],
            total_experience=screening_result["candidate_info"]["total_experience"],
            resume_url=resume_blob_url,
            resume_filename=resume_filename,
            fit_score=screening_result["fit_score"],
            must_have_skills_matched=screening_result["skills_analysis"]["must_have_matched"],
            must_have_skills_total=screening_result["skills_analysis"]["must_have_total"],
            nice_to_have_skills_matched=screening_result["skills_analysis"]["nice_to_have_matched"],
            nice_to_have_skills_total=screening_result["skills_analysis"]["nice_to_have_total"],
            matched_must_have_skills=screening_result["skills_analysis"]["matched_must_have_list"],
            matched_nice_to_have_skills=screening_result["skills_analysis"]["matched_nice_to_have_list"],
            ai_summary=ai_summary,
            skill_depth_analysis=screening_result["skill_depth_analysis"],
            professional_summary=professional_summary,
            company_tier_analysis=screening_result["company_tier_analysis"]
        )
        
        logging.info("Candidate report created")
        
        # 7. Save screening result
        logging.info("Step 7: Saving screening result to database...")
        
        screening_id = await cosmos_service.save_screening_result(
            job_id=job_id,
            user_id=user_id,
            candidate_report=candidate_report.dict()
        )
        
        logging.info(f"Saved with ID: {screening_id}")
        
        # 8. Update screening job progress
        logging.info("Step 8: Updating progress tracker...")
        
        await cosmos_service.update_screening_job_progress_by_job_id(
            job_id=job_id,
            resume_filename=resume_filename,
            status="success",
            screening_id=screening_id
        )
        
        logging.info(f"SUCCESS - {screening_result['candidate_info']['name']}")
    
    except Exception as e:
        error_message = f"PROCESSING FAILED - Error: {str(e)}, Filename: {resume_filename}, Job ID: {job_id}"
        logging.error(error_message)
        logging.error(f"Error type: {type(e).__name__}")
        
        try:
            cosmos_service_fallback = CosmosDBService()
            await cosmos_service_fallback.update_screening_job_progress_by_job_id(
                job_id=job_id,
                resume_filename=resume_filename,
                status="failed"
            )
        except Exception as fallback_error:
            logging.error(f"Failed to update screening job status: {str(fallback_error)}")
        
        # Raise a new exception with just the message to avoid serialization issues
        raise Exception(error_message)


@app.service_bus_queue_trigger(
    arg_name="msg",
    queue_name=QUEUE_NAME,
    connection="AZURE_SERVICE_BUS_CONNECTION_STRING"
)
async def resume_screening_worker(msg: func.ServiceBusMessage):
    """
    Service Bus Queue Trigger for resume screening
    Processes messages from Azure Service Bus Queue
    """
    logging.info(f"Resume Screening Worker triggered - Message ID: {msg.message_id}")
    
    try:
        # Get message body
        message_body_str = msg.get_body().decode('utf-8')
        message_body = json.loads(message_body_str)
        
        logging.info(f"Processing message: {message_body_str[:200]}")
        
        # Parse Event Grid message
        message_data = parse_event_grid_message(message_body)
        
        job_id = message_data["job_id"]
        resume_filename = message_data["resume_filename"]
        
        logging.info(f"Parsed message - Job ID: {job_id}, Resume: {resume_filename}")
        
        # Process the resume
        await process_resume_message(message_data)
        
        logging.info(f"Successfully processed resume: {resume_filename}")
        
    except Exception as e:
        error_message = f"Error processing Service Bus message: {str(e)}"
        error_type = type(e).__name__
        logging.error(f"{error_message} (Type: {error_type})")
        
        # Raise a clean exception to avoid serialization issues
        raise Exception(f"{error_type}: {str(e)}")