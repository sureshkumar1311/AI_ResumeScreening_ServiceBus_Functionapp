"""
Pydantic models for API request and response validation
"""

from pydantic import BaseModel, Field, EmailStr
from typing import List, Optional, Dict, Union
from datetime import datetime


# ==================== USER AUTHENTICATION MODELS ====================

class UserRegister(BaseModel):
    """User registration request"""
    email: EmailStr = Field(..., description="User email address")
    password: str = Field(..., min_length=8, description="Password (minimum 8 characters)")
    full_name: str = Field(..., min_length=2, description="Full name")
    company_name: Optional[str] = Field(None, description="Company name (optional)")


class UserLogin(BaseModel):
    """User login request"""
    email: EmailStr = Field(..., description="User email address")
    password: str = Field(..., description="Password")


class UserResponse(BaseModel):
    """User response (without password)"""
    user_id: str
    email: str
    full_name: str
    company_name: Optional[str] = None
    created_at: str
    is_active: bool
    total_jobs: int = 0
    total_screenings: int = 0


class LoginResponse(BaseModel):
    """Login response with JWT token"""
    access_token: str
    token_type: str = "bearer"
    user: UserResponse


# ==================== JOB DESCRIPTION MODELS ====================

# ==================== JOB DESCRIPTION MODELS ====================

class JobDescriptionRequest(BaseModel):
    """Job description upload request - JSON body"""
    screening_name: str = Field(..., description="Name/title for this screening")
    job_description_file: Optional[str] = Field(
        None, 
        description="Base64 encoded file content (PDF or DOCX). Must include data URI prefix like 'data:application/pdf;base64,' or just the base64 string. Either this or description must be provided."
    )
    description: Optional[str] = Field(
        None,
        description="Manual job description text. Either this or job_description_file must be provided."
    )


class JobDescriptionResponse(BaseModel):
    """Job description upload response"""
    job_id: str = Field(..., description="Unique job ID")
    message: str
    blob_url: Optional[str] = Field(None, description="Azure Blob Storage URL for job description")
    must_have_skills: List[str] = Field(..., description="Auto-extracted must-have technical skills")
    nice_to_have_skills: List[str] = Field(..., description="Auto-extracted nice-to-have technical skills")

# Add these models at the end of models.py

# ==================== JOB LISTING & FILTER MODELS ====================

class JobListingRequest(BaseModel):
    """Request model for job listing with filters"""
    search: Optional[str] = Field(None, description="Search term for screening_name or job_description_text")
    pageNumber: int = Field(1, ge=1, description="Page number (starts from 1)")
    pageSize: int = Field(10, ge=1, le=100, description="Number of items per page (max 100)")
    sortBy: Optional[str] = Field(
        "recent", 
        description="Sort order: 'recent' (newest first), 'oldest', 'week' (last 7 days), 'month' (last 30 days), 'name' (alphabetical)"
    )


class JobListingResponse(BaseModel):
    """Response model for job listing with pagination"""
    total_jobs: int = Field(..., description="Total number of jobs matching filters")
    total_pages: int = Field(..., description="Total number of pages")
    current_page: int = Field(..., description="Current page number")
    page_size: int = Field(..., description="Number of items per page")
    jobs: List[Dict] = Field(..., description="List of job descriptions")

class FitScore(BaseModel):
    """Fit score details"""
    score: int = Field(..., ge=0, le=100, description="Overall fit score percentage")
    reasoning: str = Field(..., description="Brief explanation of the score")


class MatchedSkill(BaseModel):
    """Matched skill details"""
    skill: str
    found_in_resume: bool
    proficiency_level: Optional[str] = None
    years_of_experience: Optional[str] = None


class SkillsAnalysis(BaseModel):
    """Skills matching analysis"""
    must_have_matched: int
    must_have_total: int
    nice_to_have_matched: int
    nice_to_have_total: int
    matched_must_have_list: List[MatchedSkill]
    matched_nice_to_have_list: List[MatchedSkill]


class SkillDepth(BaseModel):
    """Skill depth analysis for individual skill"""
    skill_name: str
    proficiency_percentage: int = Field(..., ge=0, le=100)
    evidence: Optional[str] = Field(
        None,
        description="Brief evidence from resume supporting this proficiency"
    )


class CareerGap(BaseModel):
    """Career gap details"""
    duration: str = Field(..., description="Duration of gap (e.g., '2 years 3 months')")
    reason: Optional[str] = Field(None, description="Reason for career gap if mentioned")


class IndustryExposure(BaseModel):
    """Industry exposure details"""
    industry: str
    percentage: int = Field(..., ge=0, le=100)


class ProfessionalSummary(BaseModel):
    """Professional summary details"""
    average_job_tenure: str = Field(..., description="Average tenure (e.g., '3 years 6 months')")
    tenure_assessment: str = Field(
        ...,
        description="Assessment of tenure stability (Low/Moderate/High/Very High)"
    )
    career_gap: Optional[CareerGap] = None
    major_industry_exposure: List[IndustryExposure]
    total_companies: int


class CompanyTierAnalysis(BaseModel):
    """Company tier distribution"""
    startup_percentage: int = Field(..., ge=0, le=100)
    mid_size_percentage: int = Field(..., ge=0, le=100)
    enterprise_percentage: int = Field(..., ge=0, le=100)


class CandidateInfo(BaseModel):
    """Basic candidate information"""
    name: str
    position: str
    location: Optional[str] = None
    total_experience: str


class CandidateReport(BaseModel):
    """Complete candidate screening report"""
    candidate_name: str
    email: Optional[str] = Field(None, description="Candidate email address")
    phone: Optional[str] = Field(None, description="Candidate phone number")
    position: str
    location: Optional[str] = None
    total_experience: str
    resume_url: str = Field(..., description="Azure Blob Storage URL for resume")
    resume_filename: str
    
    # Fit Score
    fit_score: FitScore
    
    # Skills Analysis
    must_have_skills_matched: int
    must_have_skills_total: int
    nice_to_have_skills_matched: int
    nice_to_have_skills_total: int
    matched_must_have_skills: List[MatchedSkill]
    matched_nice_to_have_skills: List[MatchedSkill]
    
    # AI Summary (3-4 bullet points)
    ai_summary: List[str] = Field(
        ...,
        description="AI-generated summary points about the candidate",
        min_items=3,
        max_items=5
    )
    
    # Skill Depth Analysis
    skill_depth_analysis: List[SkillDepth] = Field(
        ...,
        description="Detailed analysis of top 6-8 skills"
    )
    
    # Professional Summary
    professional_summary: ProfessionalSummary
    
    # Company Tier Analysis
    company_tier_analysis: CompanyTierAnalysis


class ResumeScreeningResponse(BaseModel):
    """Response for resume screening endpoint"""
    job_id: str
    total_resumes_processed: int
    candidates: List[CandidateReport]
    processing_timestamp: str


class ErrorResponse(BaseModel):
    """Error response model"""
    error: str
    detail: str
    timestamp: str

# Add these models at the end of models.py

# ==================== USER STATISTICS MODELS ====================

class UserStatisticsResponse(BaseModel):
    """User statistics response"""
    user_id: str
    total_job_descriptions: int = Field(..., description="Total number of job descriptions uploaded by user")
    total_resumes_screened: int = Field(..., description="Total number of resumes screened across all jobs")
    total_jobs_with_screenings: int = Field(..., description="Number of jobs that have at least one screening")
    jobs_summary: List[Dict] = Field(..., description="Summary of each job with screening count")


# ==================== RESUME SCREENING WITH BASE64 MODELS ====================

class ResumeBase64(BaseModel):
    """Single resume in base64 format"""
    resume_file: str = Field(..., description="Base64 encoded resume content (with or without data URI prefix)")
    filename: Optional[str] = Field(None, description="Original filename (optional, will be auto-generated if not provided)")


class ResumeScreeningRequest(BaseModel):
    """Request model for screening resumes"""
    job_id: str = Field(..., description="Job ID to screen resumes against")
    resumes: List[ResumeBase64] = Field(
        None, 
        min_items=1, 
        description="List of resumes in base64 format (optional if blob_urls provided)"
    )
    blob_urls: List[Dict[str, str]] = Field(
        None,
        description="List of {blob_url, filename} for already-uploaded resumes (optional if resumes provided)"
    )

class ResumeScreeningResponse(BaseModel):
    """Response for resume screening endpoint"""
    job_id: str
    total_resumes_processed: int
    candidates: List[CandidateReport]
    processing_timestamp: str
    processing_time_seconds: float = Field(..., description="Total time taken to process all resumes in seconds")