"""
AI Screening Service using Azure OpenAI
Performs intelligent resume screening and analysis with IMPROVED scoring
"""

from openai import AzureOpenAI
from config import settings
import json
import re
from typing import List, Dict, Any, Tuple


class AIScreeningService:
    """Service for AI-powered resume screening with improved fit scoring"""
    
    def __init__(self):
        """Initialize Azure OpenAI client"""
        self.client = AzureOpenAI(
            api_key=settings.AZURE_OPENAI_API_KEY,
            api_version=settings.AZURE_OPENAI_API_VERSION,
            azure_endpoint=settings.AZURE_OPENAI_ENDPOINT
        )
        self.deployment_name = settings.AZURE_OPENAI_DEPLOYMENT_NAME
    
    async def extract_skills_from_jd(self, job_description_text: str) -> Tuple[List[str], List[str]]:
        """
        Extract must-have and nice-to-have technical skills from job description
        
        Args:
            job_description_text: Complete job description text
        
        Returns:
            Tuple of (must_have_skills, nice_to_have_skills)
        """
        prompt = f"""
        Analyze this job description and extract ONLY technical skills, tools, technologies, and programming languages.
        
        RULES:
        1. Extract ONLY technical skills (languages, frameworks, tools, technologies, platforms)
        2. DO NOT include: years of experience, soft skills, education requirements, certifications
        3. Categorize into:
           - Must-have: Core technical requirements explicitly stated as required/mandatory
           - Nice-to-have: Preferred/bonus technical skills
        
        Examples of what TO include:
        - Programming languages: Python, Java, JavaScript, C++
        - Frameworks: React, Angular, Django, Spring Boot
        - Tools: Git, Docker, Kubernetes, Jenkins
        - Technologies: REST APIs, GraphQL, Microservices
        - Platforms: AWS, Azure, GCP
        - Databases: PostgreSQL, MongoDB, MySQL
        
        Examples of what NOT to include:
        - "5+ years experience"
        - "Bachelor's degree"
        - "Strong communication skills"
        - "Team player"
        - "PMP certification"
        
        Job Description:
        {job_description_text}
        
        Return ONLY a JSON object:
        {{
            "must_have_skills": ["skill1", "skill2", ...],
            "nice_to_have_skills": ["skill1", "skill2", ...]
        }}
        
        If you cannot clearly distinguish, put more critical/frequently mentioned skills in must_have.
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": "You are an expert at analyzing job descriptions and extracting technical requirements. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=2000
            )
            
            content = response.choices[0].message.content.strip()
            content = re.sub(r'```json\n?|\n?```', '', content)
            
            result = json.loads(content)
            
            must_have = result.get("must_have_skills", [])
            nice_to_have = result.get("nice_to_have_skills", [])
            
            return must_have, nice_to_have
        
        except Exception as e:
            print(f"Error extracting skills: {str(e)}")
            # Return empty lists if extraction fails
            return [], []
    
    async def screen_candidate(
        self,
        resume_text: str,
        job_description: str,
        must_have_skills: List[str],
        nice_to_have_skills: List[str]
    ) -> Dict[str, Any]:
        """
        Screen candidate resume against job requirements
        
        Args:
            resume_text: Parsed resume text
            job_description: Job description text
            must_have_skills: List of must-have technical skills (auto-extracted)
            nice_to_have_skills: List of nice-to-have technical skills (auto-extracted)
        
        Returns:
            Comprehensive screening analysis
        """
        try:
            # Extract candidate basic info
            candidate_info = await self._extract_candidate_info(resume_text)
            
            # Analyze skills match
            skills_analysis = await self._analyze_skills_match(
                resume_text,
                must_have_skills,
                nice_to_have_skills
            )
            
            # Calculate fit score (NEW: comprehensive analysis without heavy skill weighting)
            fit_score = await self._calculate_comprehensive_fit_score(
                resume_text,
                job_description,
                skills_analysis
            )
            
            # Generate AI summary
            ai_summary = await self._generate_ai_summary(
                resume_text,
                job_description,
                skills_analysis
            )
            
            # Analyze skill depth
            skill_depth_analysis = await self._analyze_skill_depth(
                resume_text,
                skills_analysis["matched_must_have_list"],
                top_n=settings.TOP_SKILLS_FOR_DEPTH_ANALYSIS
            )
            
            # Analyze professional summary
            professional_summary = await self._analyze_professional_summary(resume_text)
            
            # Analyze company tiers
            company_tier_analysis = await self._analyze_company_tiers(resume_text)
            
            return {
                "candidate_info": candidate_info,
                "fit_score": fit_score,
                "skills_analysis": skills_analysis,
                "ai_summary": ai_summary,
                "skill_depth_analysis": skill_depth_analysis,
                "professional_summary": professional_summary,
                "company_tier_analysis": company_tier_analysis
            }
        
        except Exception as e:
            raise Exception(f"Failed to screen candidate: {str(e)}")
    
    async def _extract_candidate_info(self, resume_text: str) -> Dict[str, str]:
        """Extract basic candidate information including contact details"""
        
        prompt = f"""
        Extract the following information from this resume:
        - Full name
        - Email address
        - Phone number
        - Current/desired position/title
        - Location (city, state/country)
        - Total work experience (in format: "X years Y months")
        
        Resume (full content):
        {resume_text}
        
        Return ONLY a JSON object with keys: name, email, phone, position, location, total_experience.
        If information is not found, use "Not specified".
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": "You are an expert resume parser. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=800
            )
            
            content = response.choices[0].message.content.strip()
            content = re.sub(r'```json\n?|\n?```', '', content)
            
            result = json.loads(content)
            return result
        
        except Exception as e:
            return {
                "name": "Unknown",
                "email": "Not specified",
                "phone": "Not specified",
                "position": "Not specified",
                "location": "Not specified",
                "total_experience": "Not specified"
            }
    
    async def _analyze_skills_match(
        self,
        resume_text: str,
        must_have_skills: List[str],
        nice_to_have_skills: List[str]
    ) -> Dict[str, Any]:
        """Analyze which skills match from the resume"""
        
        prompt = f"""
        Analyze this resume and determine which skills from the given lists are present.
        
        IMPORTANT INSTRUCTIONS FOR CONSISTENT RESULTS:
        1. Mark a skill as "found": true ONLY if there is CLEAR evidence in the resume
        2. Consider variations and related technologies (e.g., "React.js" matches "React", "Python3" matches "Python")
        3. Look for the skill in work experience, projects, skills sections, or certifications
        4. Be consistent: if a skill is explicitly mentioned or clearly demonstrated, mark it as found
        5. For proficiency and years: base on actual project duration and role complexity
        
        For each skill found, estimate:
        - Proficiency level: Beginner (0-1 years), Intermediate (1-3 years), Advanced (3-5 years), Expert (5+ years)
        - Years of experience: based on duration mentioned in projects/roles using that skill
        
        Resume (complete content):
        {resume_text}
        
        Must-have skills to check: {', '.join(must_have_skills) if must_have_skills else 'None'}
        Nice-to-have skills to check: {', '.join(nice_to_have_skills) if nice_to_have_skills else 'None'}
        
        Return a JSON object with this structure:
        {{
            "must_have_matched": [
                {{
                    "skill": "skill name",
                    "found": true/false,
                    "proficiency_level": "Beginner/Intermediate/Advanced/Expert",
                    "years_of_experience": "0-1 years" or "2-3 years" etc
                }}
            ],
            "nice_to_have_matched": [same structure]
        }}
        
        Return ONLY valid JSON. Be thorough and consistent in your analysis.
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": "You are an expert technical recruiter analyzing resumes. Return only valid JSON. Be consistent and thorough."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=4000
            )
            
            content = response.choices[0].message.content.strip()
            content = re.sub(r'```json\n?|\n?```', '', content)
            
            result = json.loads(content)
            
            # Process results
            must_have_matched_list = []
            must_have_matched_count = 0
            
            for skill_match in result.get("must_have_matched", []):
                skill_obj = {
                    "skill": skill_match["skill"],
                    "found_in_resume": skill_match.get("found", False),
                    "proficiency_level": skill_match.get("proficiency_level"),
                    "years_of_experience": skill_match.get("years_of_experience")
                }
                must_have_matched_list.append(skill_obj)
                if skill_match.get("found", False):
                    must_have_matched_count += 1
            
            nice_to_have_matched_list = []
            nice_to_have_matched_count = 0
            
            for skill_match in result.get("nice_to_have_matched", []):
                skill_obj = {
                    "skill": skill_match["skill"],
                    "found_in_resume": skill_match.get("found", False),
                    "proficiency_level": skill_match.get("proficiency_level"),
                    "years_of_experience": skill_match.get("years_of_experience")
                }
                nice_to_have_matched_list.append(skill_obj)
                if skill_match.get("found", False):
                    nice_to_have_matched_count += 1
            
            return {
                "must_have_matched": must_have_matched_count,
                "must_have_total": len(must_have_skills),
                "nice_to_have_matched": nice_to_have_matched_count,
                "nice_to_have_total": len(nice_to_have_skills),
                "matched_must_have_list": must_have_matched_list,
                "matched_nice_to_have_list": nice_to_have_matched_list
            }
        
        except Exception as e:
            return {
                "must_have_matched": 0,
                "must_have_total": len(must_have_skills),
                "nice_to_have_matched": 0,
                "nice_to_have_total": len(nice_to_have_skills),
                "matched_must_have_list": [],
                "matched_nice_to_have_list": []
            }
    
    async def _calculate_comprehensive_fit_score(
        self,
        resume_text: str,
        job_description: str,
        skills_analysis: Dict
    ) -> Dict[str, Any]:
        """
        Calculate comprehensive fit score based on OVERALL match, not just skills
        This addresses the issue of low scores for good candidates
        """
        
        prompt = f"""
        You are an expert recruiter evaluating how well this candidate matches the job requirements.
        Provide a comprehensive fit score from 0-100 based on the COMPLETE picture.
        
        CRITICAL SCORING GUIDELINES:
        
        **90-100 (Exceptional Match):**
        - Exceeds most job requirements significantly
        - 5+ years relevant experience for senior roles, 3+ for mid-level
        - Demonstrates deep expertise in core technical areas
        - Has worked on similar projects/domains
        - Strong career progression and achievements
        
        **75-89 (Strong Match):**
        - Meets all major requirements well
        - Relevant experience level matches job needs
        - Good technical skill coverage
        - Relevant industry/domain experience
        - Clear evidence of capability
        
        **60-74 (Good Match):**
        - Meets most key requirements
        - May lack 1-2 secondary requirements
        - Reasonable experience level
        - Transferable skills present
        - Could succeed with some ramp-up
        
        **45-59 (Moderate Match):**
        - Meets some requirements
        - May have less experience than preferred
        - Some skill gaps in secondary areas
        - Would need training/development
        
        **30-44 (Weak Match):**
        - Meets few requirements
        - Significant experience or skill gaps
        - Different domain/industry background
        - Major gaps in core competencies
        
        **0-29 (Poor Match):**
        - Minimal alignment
        - Wrong career level or domain
        - Missing most critical requirements
        
        EVALUATION FACTORS (weight them appropriately):
        1. **Technical Skills (30%)**: How many relevant technical skills does candidate have?
        2. **Experience Level (25%)**: Does years of experience match job requirements?
        3. **Role Relevance (20%)**: How similar is past work to this job's responsibilities?
        4. **Domain Knowledge (15%)**: Relevant industry/domain experience?
        5. **Career Trajectory (10%)**: Shows growth and increasing responsibility?
        
        IMPORTANT: 
        - Don't penalize heavily for missing a few nice-to-have skills if overall profile is strong
        - Focus on transferable experience and learning ability
        - Consider the WHOLE picture, not just a checklist
        - Be realistic but fair - a 70-80% match is actually quite good!
        
        Job Description (complete):
        {job_description}
        
        Resume (complete):
        {resume_text}
        
        Skills Analysis:
        - Must-have skills matched: {skills_analysis['must_have_matched']} of {skills_analysis['must_have_total']}
        - Nice-to-have skills matched: {skills_analysis['nice_to_have_matched']} of {skills_analysis['nice_to_have_total']}
        
        Return ONLY a JSON object:
        {{
            "score": <number 0-100>,
            "reasoning": "<2-3 sentence explanation of the score>"
        }}
        
        Be objective, thorough, and fair in your assessment.
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": "You are an expert recruiter who provides fair, comprehensive, and accurate candidate assessments. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=800
            )
            
            content = response.choices[0].message.content.strip()
            content = re.sub(r'```json\n?|\n?```', '', content)
            result = json.loads(content)
            
            score = min(100, max(0, result.get("score", 50)))
            reasoning = result.get("reasoning", "Score based on overall profile match")
            
            return {
                "score": int(score),
                "reasoning": reasoning
            }
        
        except Exception as e:
            print(f"Error calculating fit score: {str(e)}")
            return {
                "score": 50,
                "reasoning": "Unable to calculate detailed fit score. Manual review recommended."
            }
    
    async def _generate_ai_summary(
        self,
        resume_text: str,
        job_description: str,
        skills_analysis: Dict
    ) -> List[str]:
        """Generate AI summary points about the candidate"""
        
        prompt = f"""
        Create 3-4 concise bullet points summarizing this candidate's strengths and fit for the role.
        Focus on: key technical skills, experience relevance, notable achievements, and unique strengths.
        
        Be objective and base your summary on concrete evidence from the resume.
        
        IMPORTANT: You MUST return at least 3 bullet points. If information is limited, create 3 general points about the candidate.
        
        Job Requirements (complete):
        {job_description}
        
        Resume (complete):
        {resume_text}
        
        Return ONLY a JSON array of strings: ["point 1", "point 2", "point 3", "point 4"]
        Each point should be 1-2 sentences and focus on factual information from the resume.
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": "You are an expert recruiter providing objective candidate summaries. Return only valid JSON array with at least 3 items."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=800
            )
            
            content = response.choices[0].message.content.strip()
            content = re.sub(r'```json\n?|\n?```', '', content)
            
            summary_points = json.loads(content)
            
            #  Ensure we have at least 3 points
            if not summary_points or len(summary_points) < 3:
                # Fallback summary
                summary_points = [
                    f"Candidate has relevant background in {skills_analysis.get('matched_must_have_list', [{}])[0].get('skill', 'the field') if skills_analysis.get('matched_must_have_list') else 'the field'}",
                    f"Demonstrates experience with {skills_analysis.get('must_have_matched', 0)} of {skills_analysis.get('must_have_total', 0)} required skills",
                    "Please review the detailed resume for comprehensive assessment"
                ]
            
            return summary_points[:4]
        
        except Exception as e:
            print(f"Error generating AI summary: {str(e)}")
            # Return fallback summary
            return [
                "Candidate profile reviewed for position requirements",
                "Technical skills assessment completed",
                "Please review detailed screening results for complete evaluation"
            ]
    
    async def _analyze_skill_depth(
        self,
        resume_text: str,
        matched_skills: List[Dict],
        top_n: int = 6
    ) -> List[Dict[str, Any]]:
        """Analyze proficiency depth for top skills"""
        
        found_skills = [s for s in matched_skills if s["found_in_resume"]][:top_n]
        
        if not found_skills:
            return []
        
        skills_list = [s["skill"] for s in found_skills]
        
        prompt = f"""
        For each skill, estimate the candidate's proficiency percentage (0-100) based on their resume.
        
        PROFICIENCY SCORING GUIDELINES (be consistent):
        - 0-25%: Beginner - mentioned briefly, minimal experience
        - 26-50%: Intermediate - used in 1-2 projects, 1-2 years experience
        - 51-75%: Advanced - used extensively, 3-5 years, led projects
        - 76-100%: Expert - deep expertise, 5+ years, mentored others, complex projects
        
        Consider: years of experience, project complexity, leadership roles, certifications, depth of usage.
        
        Skills to analyze: {', '.join(skills_list)}
        
        Resume (complete content):
        {resume_text}
        
        Return ONLY a JSON array:
        [
            {{"skill_name": "skill", "proficiency_percentage": number, "evidence": "brief evidence from resume"}},
            ...
        ]
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": "You are an expert at assessing technical skills objectively. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=3000
            )
            
            content = response.choices[0].message.content.strip()
            content = re.sub(r'```json\n?|\n?```', '', content)
            
            result = json.loads(content)
            
            for item in result:
                item["proficiency_percentage"] = min(100, max(0, item.get("proficiency_percentage", 50)))
            
            return result
        
        except Exception as e:
            return [
                {
                    "skill_name": skill["skill"],
                    "proficiency_percentage": 50,
                    "evidence": "Unable to assess automatically"
                }
                for skill in found_skills
            ]
    
    async def _analyze_professional_summary(self, resume_text: str) -> Dict[str, Any]:
        """Analyze professional summary including tenure, gaps, industry exposure"""
        
        prompt = f"""
        Analyze this resume and provide:
        1. Average job tenure (format: "X years Y months")
        2. Tenure assessment (Low/Moderate/High/Very High based on average tenure)
        3. Career gap if any (duration and reason if mentioned)
        4. Industry exposure percentages (identify top industries and their percentage distribution)
        5. Total number of companies worked for
        
        IMPORTANT: 
        - If no significant career gap (>6 months) is found, return null for career_gap
        - If a career gap exists, ALWAYS provide a duration string like "6 months" or "1 year"
        - Never return empty string for duration
        
        Resume (complete content):
        {resume_text}
        
        Return ONLY a JSON object:
        {{
            "average_job_tenure": "X years Y months",
            "tenure_assessment": "Low/Moderate/High/Very High",
            "career_gap": {{"duration": "X years Y months", "reason": "reason"}} or null,
            "industry_exposure": [
                {{"industry": "name", "percentage": number}},
                ...
            ],
            "total_companies": number
        }}
        
        For career_gap, return null if no significant gap found.
        Industry percentages should sum to 100.
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": "You are an expert at analyzing career histories. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=1500
            )
            
            content = response.choices[0].message.content.strip()
            content = re.sub(r'```json\n?|\n?```', '', content)
            
            result = json.loads(content)
            
            #  Validate career_gap structure
            career_gap = result.get("career_gap")
            if career_gap:
                # Ensure duration is a valid string
                if not career_gap.get("duration") or career_gap.get("duration") == "":
                    career_gap = None
                elif not isinstance(career_gap.get("duration"), str):
                    career_gap = None
            
            return {
                "average_job_tenure": result.get("average_job_tenure", "Not specified"),
                "tenure_assessment": result.get("tenure_assessment", "Moderate"),
                "career_gap": career_gap,
                "major_industry_exposure": result.get("industry_exposure", []),
                "total_companies": result.get("total_companies", 0)
            }
        
        except Exception as e:
            print(f"Error analyzing professional summary: {str(e)}")
            return {
                "average_job_tenure": "Not specified",
                "tenure_assessment": "Moderate",
                "career_gap": None,
                "major_industry_exposure": [],
                "total_companies": 0
            }
    
    async def _analyze_company_tiers(self, resume_text: str) -> Dict[str, int]:
        """Analyze distribution of company tiers"""
        
        prompt = f"""
        Analyze the companies mentioned in this resume and classify them into:
        - Startup (small companies, typically <100 employees)
        - Mid-size (medium companies, 100-1000 employees)
        - Enterprise (large corporations, >1000 employees)
        
        Provide percentage distribution that sums to 100.
        
        Resume (complete content):
        {resume_text}
        
        Return ONLY a JSON object:
        {{
            "startup_percentage": number,
            "mid_size_percentage": number,
            "enterprise_percentage": number
        }}
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": "You are an expert at analyzing companies. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=400
            )
            
            content = response.choices[0].message.content.strip()
            content = re.sub(r'```json\n?|\n?```', '', content)
            
            result = json.loads(content)
            
            total = result.get("startup_percentage", 0) + result.get("mid_size_percentage", 0) + result.get("enterprise_percentage", 0)
            
            if total == 0:
                return {
                    "startup_percentage": 33,
                    "mid_size_percentage": 34,
                    "enterprise_percentage": 33
                }
            
            factor = 100 / total
            return {
                "startup_percentage": int(result.get("startup_percentage", 0) * factor),
                "mid_size_percentage": int(result.get("mid_size_percentage", 0) * factor),
                "enterprise_percentage": int(result.get("enterprise_percentage", 0) * factor)
            }
        
        except Exception as e:
            return {
                "startup_percentage": 33,
                "mid_size_percentage": 34,
                "enterprise_percentage": 33
            }