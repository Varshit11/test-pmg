from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import json
import uvicorn
import os
import pickle
import numpy as np
from datetime import datetime
import logging
from similar_issues2 import GatiShaktiSimilarIssuesRAG
import re

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Gati Shakti Live API", version="1.0.0")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variables for data and embeddings
issues_database = []
embeddings_matrix = None
project_issue_index = {}  # Maps (project_id, issue_id) -> database index
rag_system = None

# Response models
class ShortSummaryResponse(BaseModel):
    project_id: str
    issue_id: str
    short_summary: str
    processing_time: float
    timestamp: str

class LongSummaryResponse(BaseModel):
    project_id: str
    issue_id: str
    detailed_summary: str
    processing_time: float
    timestamp: str

class IssueClassificationResponse(BaseModel):
    project_id: str
    issue_id: str
    issue_classification: str
    processing_time: float
    timestamp: str

class SimilarIssueItem(BaseModel):
    project_id: str
    issue_id: str
    issue_key: str
    title: str
    sector: str
    issue_category: str
    similarity_score: float
    similarity_reasons: List[str]
    llm_analysis: str
    manual_issue_type: str
    resolution_strategy: str

class SimilarIssuesResponse(BaseModel):
    project_id: str
    issue_id: str
    similar_issues: List[SimilarIssueItem]
    processing_time: float
    timestamp: str

def clean_llm_response(text: str) -> str:
    """Clean up LLM response text"""
    # Remove markdown-style bold and italics
    cleaned_text = re.sub(r'(\*\*|__|\*)', '', text)
    
    # Remove numeric points formatting (e.g., 1., 2., 3.)
    cleaned_text = re.sub(r'(\d+\.)\s+', r'\1 ', cleaned_text)
    
    # Remove excessive spaces and newlines
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
    
    # Replace multiple spaces with a single space
    cleaned_text = re.sub(r' {2,}', ' ', cleaned_text)
    
    # Normalize punctuation spacing
    cleaned_text = re.sub(r'\s+([.,:;!?])', r'\1', cleaned_text)
    
    return cleaned_text

def load_database():
    """Load issues database and create/load embeddings"""
    global issues_database, embeddings_matrix, project_issue_index, rag_system
    
    logger.info("Loading issues database...")
    
    # Load issues database
    database_file = 'data/prod/gati_shakti_feature_engineering_with_time_features.json'
    embeddings_file = 'data/prod/embeddings_cache.pkl'
    index_file = 'data/prod/project_issue_index.json'
    
    try:
        # Load issues data
        with open(database_file, 'r', encoding='utf-8') as f:
            issues_database = json.load(f)
        logger.info(f"Loaded {len(issues_database)} issues from database")
        
        # Initialize RAG system
        rag_system = GatiShaktiSimilarIssuesRAG(use_llm=True)
        
        # Try to load pre-computed embeddings
        if os.path.exists(embeddings_file) and os.path.exists(index_file):
            logger.info("Loading pre-computed embeddings...")
            with open(embeddings_file, 'rb') as f:
                embeddings_matrix = pickle.load(f)
            
            with open(index_file, 'r', encoding='utf-8') as f:
                project_issue_index = json.load(f)
            
            logger.info("Pre-computed embeddings loaded successfully")
        else:
            logger.info("Computing embeddings (this may take a few minutes)...")
            # Compute embeddings
            embeddings_matrix = rag_system.create_issue_embeddings(issues_database)
            
            # Create project-issue index
            project_issue_index = {}
            for i, issue in enumerate(issues_database):
                project_id = issue.get('projectId', '')
                issue_id = issue.get('issueId', '')
                if project_id and issue_id:
                    key = f"{project_id}_{issue_id}"
                    project_issue_index[key] = i
            
            # Save embeddings and index
            os.makedirs('data/prod', exist_ok=True)
            with open(embeddings_file, 'wb') as f:
                pickle.dump(embeddings_matrix, f)
            
            with open(index_file, 'w', encoding='utf-8') as f:
                json.dump(project_issue_index, f, indent=2)
            
            logger.info("Embeddings computed and cached successfully")
            
    except FileNotFoundError as e:
        logger.error(f"Database file not found: {e}")
        raise Exception("Database file not found. Please ensure the feature engineering JSON exists.")
    except Exception as e:
        logger.error(f"Error loading database: {e}")
        raise

def find_issue_by_ids(project_id: str, issue_id: str) -> tuple:
    """Find issue by project_id and issue_id"""
    key = f"{project_id}_{issue_id}"
    if key in project_issue_index:
        index = project_issue_index[key]
        return issues_database[index], index
    else:
        # Fallback: search through all issues
        for i, issue in enumerate(issues_database):
            if (issue.get('projectId', '') == project_id and 
                issue.get('issueId', '') == issue_id):
                return issue, i
        return None, None

@app.on_event("startup")
async def startup_event():
    """Load database on startup"""
    try:
        load_database()
        logger.info("API server ready!")
    except Exception as e:
        logger.error(f"Failed to initialize server: {e}")
        raise

@app.get("/")
async def root():
    return {
        "message": "Gati Shakti Live API Server",
        "version": "1.0.0",
        "endpoints": [
            "/short-summary/{project_id}/{issue_id}",
            "/long-summary/{project_id}/{issue_id}",
            "/issue-classification/{project_id}/{issue_id}",
            "/similar-issues/{project_id}/{issue_id}"
        ],
        "total_issues": len(issues_database) if issues_database else 0
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "database_loaded": len(issues_database) > 0,
        "embeddings_ready": embeddings_matrix is not None,
        "rag_system_ready": rag_system is not None,
        "total_issues": len(issues_database)
    }

@app.get("/short-summary/{project_id}/{issue_id}", response_model=ShortSummaryResponse)
async def get_short_summary(project_id: str, issue_id: str):
    """Generate short summary for a specific issue"""
    start_time = datetime.now()
    
    # Find the issue
    issue, index = find_issue_by_ids(project_id, issue_id)
    if issue is None:
        raise HTTPException(
            status_code=404, 
            detail=f"Issue not found for project_id: {project_id}, issue_id: {issue_id}"
        )
    
    # Prepare issue text
    issue_text = json.dumps({
        "Issue_Description": issue.get("issueDescription", ""),
        "Latest Issue Comment (Ministry/State)": issue.get("latestIssueCommentMinistryOrState", ""),
        "Latest Meeting Minutes": issue.get("latestMeetingDecision", ""),
        "Historical_Comments": [c.get("comment", "") for c in issue.get("comments", [])],
        "Historical_Actions": [d.get("meetingDecision", "") for d in issue.get("decisions", [])]
    })
    
    # Generate short summary
    short_summary_prompt = f"""You are an expert summary generator. Your task is to read the input text and produce a clear, concise, and accurate short summary in just two or three lines based on the input text. 
    Focus on the main issue, what was discussed in the last meeting and what action has to be taken. Avoid redundancy, remove filler content, and ensure the output is easy to understand.
    Make sure to mention key stakeholders, deadlines, and current status if available.

    Input text: {issue_text}

    Provide short summary:"""
    
    try:
        short_summary = rag_system._call_llm_api(short_summary_prompt)
        short_summary = clean_llm_response(short_summary)
    except Exception as e:
        logger.error(f"LLM API error for short summary: {e}")
        raise HTTPException(status_code=503, detail="AI service temporarily unavailable")
    
    processing_time = (datetime.now() - start_time).total_seconds()
    
    return ShortSummaryResponse(
        project_id=project_id,
        issue_id=issue_id,
        short_summary=short_summary,
        processing_time=processing_time,
        timestamp=datetime.now().isoformat()
    )

@app.get("/long-summary/{project_id}/{issue_id}", response_model=LongSummaryResponse)
async def get_long_summary(project_id: str, issue_id: str):
    """Generate detailed summary for a specific issue"""
    start_time = datetime.now()
    
    # Find the issue
    issue, index = find_issue_by_ids(project_id, issue_id)
    if issue is None:
        raise HTTPException(
            status_code=404, 
            detail=f"Issue not found for project_id: {project_id}, issue_id: {issue_id}"
        )
    
    # Prepare issue text
    issue_text = json.dumps({
        "Issue_Description": issue.get("issueDescription", ""),
        "Latest Issue Comment (Ministry/State)": issue.get("latestIssueCommentMinistryOrState", ""),
        "Latest Meeting Minutes": issue.get("latestMeetingDecision", ""),
        "Historical_Comments": [c.get("comment", "") for c in issue.get("comments", [])],
        "Historical_Actions": [d.get("meetingDecision", "") for d in issue.get("decisions", [])]
    })
    
    # Generate detailed summary
    long_summary_prompt = f"""You are an expert project issue summarizer. Read the input text containing issue details such as issue title, description, last meeting minutes, updates, comments, and actions, and 
    produce a single clear, consolidated paragraph that captures all key and relevant information. The summary must be detailed descriptive, professional, and precise, with no redundancy, filler, or vague phrasing. 
    Do not create separate sections or headings; instead, merge all the issue details in the consolidated way into one seamless narrative and process flow such as:
    issue description, any updates or comments on that, anything decided in the last meeting and what are the actions taken etc.
    The output should be self-contained so the reader does not need to refer to the original text, and it should only include all the important, useful context without repeating information.
    Include the complete chain of events chronologically: what is the issue → what actions were taken → current status → meeting decisions → next steps → timeline.
    Mention all key stakeholders, amounts, dates, and dependencies clearly.

    Input text: {issue_text}

    Provide detailed summary:"""
    
    try:
        detailed_summary = rag_system._call_llm_api(long_summary_prompt)
        detailed_summary = clean_llm_response(detailed_summary)
    except Exception as e:
        logger.error(f"LLM API error for detailed summary: {e}")
        raise HTTPException(status_code=503, detail="AI service temporarily unavailable")
    
    processing_time = (datetime.now() - start_time).total_seconds()
    
    return LongSummaryResponse(
        project_id=project_id,
        issue_id=issue_id,
        detailed_summary=detailed_summary,
        processing_time=processing_time,
        timestamp=datetime.now().isoformat()
    )

@app.get("/issue-classification/{project_id}/{issue_id}", response_model=IssueClassificationResponse)
async def get_issue_classification(project_id: str, issue_id: str):
    """Classify a specific issue"""
    start_time = datetime.now()
    
    # Find the issue
    issue, index = find_issue_by_ids(project_id, issue_id)
    if issue is None:
        raise HTTPException(
            status_code=404, 
            detail=f"Issue not found for project_id: {project_id}, issue_id: {issue_id}"
        )
    
    # Prepare issue text
    issue_text = json.dumps({
        "Issue_Description": issue.get("issueDescription", ""),
        "Latest Issue Comment (Ministry/State)": issue.get("latestIssueCommentMinistryOrState", ""),
        "Latest Meeting Minutes": issue.get("latestMeetingDecision", ""),
        "Historical_Comments": [c.get("comment", "") for c in issue.get("comments", [])],
        "Historical_Actions": [d.get("meetingDecision", "") for d in issue.get("decisions", [])]
    })
    
    # Classification prompt
    classification_prompt = f"""You are an expert project issue classifier for infrastructure and development projects. Based on the input data, classify the issue into ONE of the following 8 categories:

**CLASSIFICATION CATEGORIES:**

1. **LAND & ACQUISITION ISSUES**
   - Scope: Land acquisition delays, encroachment removal, possession handover, compensation disputes
   - Sub-types: Land Acquisition, Land Encroachment, Demand High Compensation
   - Keywords: land acquisition, compensation, possession, encroachment, demarcation, revenue records
   - Example: "Demarcation of land and removal of encroachment pending on the land along railway line"

2. **REGULATORY APPROVALS & PERMISSIONS**
   - Scope: Pending government approvals, NOCs, environmental clearances, statutory permissions
   - Sub-types: Approval pending, Lease pending, RoW (Right of Way) Permission
   - Keywords: approval, NOC, clearance, permission, license, statutory, regulatory
   - Example: "Pending permission/NOC by DUSIB for relocation of shops/structures"

3. **UTILITY & INFRASTRUCTURE COORDINATION**
   - Scope: Electricity connections, utility shifting, water resources, coordination with utility providers
   - Sub-types: Electricity issue, Water resources issue, Waiver of Maintenance charges
   - Keywords: utility shifting, electrical, power lines, water, telecom, infrastructure coordination
   - Example: "Shifting of electrical poles and lines going through AIIMS Rewari campus"

4. **ROADS & CONNECTIVITY**
   - Scope: Access roads, connectivity infrastructure, transportation links
   - Sub-types: Roads Construction & Connectivity
   - Keywords: access road, connectivity, transportation, highway, bridge, road construction
   - Example: "Requirement of an access road for movement of heavy vehicle and equipment"

5. **CONSTRUCTION & EXECUTION DELAYS**
   - Scope: Construction delays, contractor issues, execution bottlenecks
   - Sub-types: Construction delay
   - Keywords: construction delay, contractor, execution, timeline, project delay, building
   - Example: Issues related to project timeline delays and execution challenges

6. **FINANCIAL & DISBURSEMENT**
   - Scope: Payment delays, fund disbursement issues, financial approvals
   - Sub-types: Disbursement Issue
   - Keywords: payment, disbursement, funds, financial, budget, cost, money transfer
   - Example: Financial processing and payment-related bottlenecks

7. **ENVIRONMENTAL & COMPLIANCE**
   - Scope: Environmental clearances, forest permissions, compliance issues
   - Sub-types: Environmental approvals, wildlife clearances
   - Keywords: environmental clearance, forest, wildlife, ecology, compliance, green clearance
   - Example: Environmental and regulatory compliance matters

8. **STAKEHOLDER COORDINATION**
   - Scope: Multi-agency coordination, inter-ministerial issues, stakeholder alignment
   - Sub-types: Cross-departmental coordination issues
   - Keywords: coordination, inter-ministerial, multi-agency, stakeholder, departmental alignment
   - Example: Issues requiring coordination between multiple government bodies

**INSTRUCTIONS:**
- Analyze the issue description, comments, meeting minutes, and actions carefully
- Identify the primary bottleneck or challenge that is causing the delay
- Select the MOST APPROPRIATE single category based on the core issue
- Consider the main stakeholders involved and the type of resolution required
- Provide ONLY the exact category name as output (e.g., "UTILITY & INFRASTRUCTURE COORDINATION")

**INPUT DATA:** {issue_text}

**CLASSIFICATION:**"""
    
    try:
        issue_classification = rag_system._call_llm_api(classification_prompt)
        issue_classification = clean_llm_response(issue_classification).strip()
    except Exception as e:
        logger.error(f"LLM API error for classification: {e}")
        raise HTTPException(status_code=503, detail="AI service temporarily unavailable")
    
    processing_time = (datetime.now() - start_time).total_seconds()
    
    return IssueClassificationResponse(
        project_id=project_id,
        issue_id=issue_id,
        issue_classification=issue_classification,
        processing_time=processing_time,
        timestamp=datetime.now().isoformat()
    )

@app.get("/similar-issues/{project_id}/{issue_id}", response_model=SimilarIssuesResponse)
async def get_similar_issues(project_id: str, issue_id: str, k: int = 3):
    """Find similar issues for a specific issue"""
    start_time = datetime.now()
    
    # Find the issue
    issue, index = find_issue_by_ids(project_id, issue_id)
    if issue is None:
        raise HTTPException(
            status_code=404, 
            detail=f"Issue not found for project_id: {project_id}, issue_id: {issue_id}"
        )
    
    if embeddings_matrix is None:
        raise HTTPException(status_code=503, detail="Embeddings not ready")
    
    try:
        # Find similar issues
        similar_issues_raw = rag_system.find_similar_issues(
            query_issue=issue,
            all_issues=issues_database,
            all_embeddings=embeddings_matrix,
            query_idx=index,
            k=k
        )
        
        # Format similar issues
        similar_issues = []
        for sim in similar_issues_raw:
            similar_issue = sim['issue']
            similar_issues.append(SimilarIssueItem(
                project_id=similar_issue.get('projectId', 'unknown'),
                issue_id=similar_issue.get('issueId', 'unknown'),
                issue_key=similar_issue.get('issue_key', 'unknown'),
                title=similar_issue.get('issueTitle', 'No title'),
                sector=similar_issue.get('feat_sector', 'Unknown'),
                issue_category=similar_issue.get('feat_issue_category', 'Unknown'),
                similarity_score=sim['combined_similarity'],
                similarity_reasons=sim['match_reasons'][:5],  # Top 5 reasons
                llm_analysis=clean_llm_response(sim['llm_analysis']),
                manual_issue_type=similar_issue.get('feat_manual_issue_type', 'Unknown'),
                resolution_strategy=similar_issue.get('feat_manual_resolution_strategy', 'Unknown')
            ))
        
    except Exception as e:
        logger.error(f"Error finding similar issues: {e}")
        raise HTTPException(status_code=500, detail="Error processing similar issues")
    
    processing_time = (datetime.now() - start_time).total_seconds()
    
    return SimilarIssuesResponse(
        project_id=project_id,
        issue_id=issue_id,
        similar_issues=similar_issues,
        processing_time=processing_time,
        timestamp=datetime.now().isoformat()
    )

@app.get("/list-issues")
async def list_issues(limit: int = 10, offset: int = 0):
    """List available issues (for testing purposes)"""
    if not issues_database:
        raise HTTPException(status_code=503, detail="Database not loaded")
    
    total_issues = len(issues_database)
    issues_slice = issues_database[offset:offset + limit]
    
    result = []
    for issue in issues_slice:
        result.append({
            "project_id": issue.get('projectId', 'unknown'),
            "issue_id": issue.get('issueId', 'unknown'),
            "title": issue.get('issueTitle', 'No title')[:100],
            "sector": issue.get('feat_sector', 'Unknown')
        })
    
    return {
        "total_issues": total_issues,
        "offset": offset,
        "limit": limit,
        "issues": result
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)