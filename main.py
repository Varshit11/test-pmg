# # from fastapi import FastAPI, HTTPException, UploadFile, File
# # from fastapi.middleware.cors import CORSMiddleware
# # from pydantic import BaseModel
# # from typing import Optional, List, Dict
# # import requests
# # import json
# # import uvicorn
# # from datetime import datetime
# # import urllib3

# # # Disable SSL warnings
# # urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# # app = FastAPI(title="Gati Shakti Issue Summarizer API", version="1.0.0")

# # # Enable CORS
# # app.add_middleware(
# #     CORSMiddleware,
# #     allow_origins=["*"],
# #     allow_credentials=True,
# #     allow_methods=["*"],
# #     allow_headers=["*"],
# # )

# # # Pydantic models
# # class ManualIssueInput(BaseModel):
# #     project_id: str
# #     project_name: str
# #     issue_description: str
# #     latest_comments: Optional[str] = ""
# #     meeting_minutes: Optional[str] = ""
# #     responsible_authority: Optional[str] = ""

# # class SummaryResponse(BaseModel):
# #     short_summary: str
# #     detailed_summary: str
# #     issue_classification: str
# #     processing_time: float
# #     timestamp: str

# # class StatsResponse(BaseModel):
# #     total_projects: int = 1247
# #     active_issues: int = 324
# #     resolved_issues: int = 1893
# #     summaries_generated: int = 5621

# # def call_llm_api(prompt):
# #     """LLM API call function from original script"""
# #     url = "https://apis.airawat.cdac.in/negd/v1/chat/completions"
# #     token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJ5YUtYUGtLZ1lKSUNqRTg3dHUwVnBUS3R5UUhxaUk1cyJ9.wJbzY_YRXGBug9sO1EYSwiCkRrI1bKnOL3uxSITxV94"
# #     headers = {
# #         "Authorization": f"Bearer {token}",
# #         "Cookie": "SERVERID=api-manager",
# #         "Content-Type": "application/json"
# #     }

# #     payload = {
# #         "model": "meta/llama-3.2-11b-vision-instruct",
# #         "messages": [{"role": "user", "content": prompt}],
# #         "max_tokens": 2000
# #     }
# #     try:
# #         response = requests.post(url, headers=headers, json=payload, verify=False, timeout=30)
# #         response.raise_for_status()
# #         result = response.json()
# #         return result["choices"][0]["message"]["content"].strip()
# #     except requests.RequestException as e:
# #         raise HTTPException(status_code=500, detail=f"API request failed: {e}")

# # def generate_short_summary(issue_data):
# #     """Enhanced short summary using original prompt style"""
# #     prompt_template_short = f"""You are an expert summary generator. Your task is to read the input text and produce a clear, concise, and accurate short summary in just two or three lines based on the input text. 
# # Focus on the main issue, what was discussed in the last meeting and what action has to be taken. Avoid redundancy, remove filler content, and ensure the output is easy to understand.
# # Make sure to mention key stakeholders, deadlines, and current status if available.

# # Input text: {issue_data}

# # Provide short summary:"""
    
# #     return call_llm_api(prompt_template_short)

# # def generate_detailed_summary(issue_data):
# #     """Enhanced detailed summary using original prompt style"""
# #     prompt_template_detailed = f"""You are an expert project issue summarizer. Read the input text containing issue details such as issue title, description, last meeting minutes, updates, comments, and actions, and 
# # produce a single clear, consolidated paragraph that captures all key and relevant information. The summary must be detailed descriptive, professional, and precise, with no redundancy, filler, or vague phrasing. 
# # Do not create separate sections or headings; instead, merge all the issue details in the consolidated way into one seamless narrative and process flow such as:
# # issue description, any updates or comments on that, anything decided in the last meeting and what are the actions taken etc.
# # The output should be self-contained so the reader does not need to refer to the original text, and it should only include all the important, useful context without repeating information.
# # Include the complete chain of events chronologically: what is the issue → what actions were taken → current status → meeting decisions → next steps → timeline.
# # Mention all key stakeholders, amounts, dates, and dependencies clearly.

# # Input text: {issue_data}

# # Provide detailed summary:"""
    
# #     return call_llm_api(prompt_template_detailed)

# # def classify_project_issue(issue_data):
# #     """Detailed issue classification"""
# #     prompt_classification = f"""You are an expert project issue classifier for infrastructure and development projects. Based on the input data, classify the issue into ONE of the following 8 categories:

# # **CLASSIFICATION CATEGORIES:**

# # 1. **LAND & ACQUISITION ISSUES**
# #    - Scope: Land acquisition delays, encroachment removal, possession handover, compensation disputes
# #    - Sub-types: Land Acquisition, Land Encroachment, Demand High Compensation
# #    - Keywords: land acquisition, compensation, possession, encroachment, demarcation, revenue records
# #    - Example: "Demarcation of land and removal of encroachment pending on the land along railway line"

# # 2. **REGULATORY APPROVALS & PERMISSIONS**
# #    - Scope: Pending government approvals, NOCs, environmental clearances, statutory permissions
# #    - Sub-types: Approval pending, Lease pending, RoW (Right of Way) Permission
# #    - Keywords: approval, NOC, clearance, permission, license, statutory, regulatory
# #    - Example: "Pending permission/NOC by DUSIB for relocation of shops/structures"

# # 3. **UTILITY & INFRASTRUCTURE COORDINATION**
# #    - Scope: Electricity connections, utility shifting, water resources, coordination with utility providers
# #    - Sub-types: Electricity issue, Water resources issue, Waiver of Maintenance charges
# #    - Keywords: utility shifting, electrical, power lines, water, telecom, infrastructure coordination
# #    - Example: "Shifting of electrical poles and lines going through AIIMS Rewari campus"

# # 4. **ROADS & CONNECTIVITY**
# #    - Scope: Access roads, connectivity infrastructure, transportation links
# #    - Sub-types: Roads Construction & Connectivity
# #    - Keywords: access road, connectivity, transportation, highway, bridge, road construction
# #    - Example: "Requirement of an access road for movement of heavy vehicle and equipment"

# # 5. **CONSTRUCTION & EXECUTION DELAYS**
# #    - Scope: Construction delays, contractor issues, execution bottlenecks
# #    - Sub-types: Construction delay
# #    - Keywords: construction delay, contractor, execution, timeline, project delay, building
# #    - Example: Issues related to project timeline delays and execution challenges

# # 6. **FINANCIAL & DISBURSEMENT**
# #    - Scope: Payment delays, fund disbursement issues, financial approvals
# #    - Sub-types: Disbursement Issue
# #    - Keywords: payment, disbursement, funds, financial, budget, cost, money transfer
# #    - Example: Financial processing and payment-related bottlenecks

# # 7. **ENVIRONMENTAL & COMPLIANCE**
# #    - Scope: Environmental clearances, forest permissions, compliance issues
# #    - Sub-types: Environmental approvals, wildlife clearances
# #    - Keywords: environmental clearance, forest, wildlife, ecology, compliance, green clearance
# #    - Example: Environmental and regulatory compliance matters

# # 8. **STAKEHOLDER COORDINATION**
# #    - Scope: Multi-agency coordination, inter-ministerial issues, stakeholder alignment
# #    - Sub-types: Cross-departmental coordination issues
# #    - Keywords: coordination, inter-ministerial, multi-agency, stakeholder, departmental alignment
# #    - Example: Issues requiring coordination between multiple government bodies

# # **INSTRUCTIONS:**
# # - Analyze the issue description, comments, meeting minutes, and actions carefully
# # - Identify the primary bottleneck or challenge that is causing the delay
# # - Select the MOST APPROPRIATE single category based on the core issue
# # - Consider the main stakeholders involved and the type of resolution required
# # - Provide ONLY the exact category name as output (e.g., "UTILITY & INFRASTRUCTURE COORDINATION")

# # **INPUT DATA:** {issue_data}

# # **CLASSIFICATION:**"""
    
# #     return call_llm_api(prompt_classification)

# # def extract_issues(data, selected_keys):
# #     """Extract issues from JSON data"""
# #     if not data or "Project" not in data or "Issues" not in data["Project"]:
# #         raise HTTPException(status_code=400, detail="Invalid JSON structure")
    
# #     issues = data["Project"]["Issues"]
# #     if not issues:
# #         raise HTTPException(status_code=400, detail="No issues found in JSON")
    
# #     issue_subdict = [
# #         {k: issue[k] for k in selected_keys if k in issue}
# #         for issue in issues
# #     ]
# #     return issue_subdict

# # @app.get("/")
# # async def root():
# #     return {"message": "Gati Shakti Issue Summarizer API is running"}

# # @app.get("/stats", response_model=StatsResponse)
# # async def get_stats():
# #     return StatsResponse()

# # @app.post("/process-json", response_model=SummaryResponse)
# # async def process_json_file(file: UploadFile = File(...)):
# #     if not file.filename.endswith('.json'):
# #         raise HTTPException(status_code=400, detail="Only JSON files are allowed")
    
# #     try:
# #         start_time = datetime.now()
        
# #         # Read JSON file
# #         content = await file.read()
# #         json_data = json.loads(content.decode('utf-8'))
        
# #         # Selected keys from original script
# #         selected_keys = [
# #             "Issue_Description",
# #             "Latest Issue Comment (Ministry/State)",
# #             "Latest Meeting Minutes",
# #             "Historical_Comments",
# #             "Historical_Updates",
# #             "Historical_Actions"
# #         ]
        
# #         # Extract issues
# #         issues_subset = extract_issues(json_data, selected_keys)
        
# #         # Generate summaries using original approach
# #         short_summary = generate_short_summary(issues_subset)
# #         detailed_summary = generate_detailed_summary(issues_subset)
# #         issue_classification = classify_project_issue(issues_subset)
        
# #         end_time = datetime.now()
# #         processing_time = (end_time - start_time).total_seconds()
        
# #         return SummaryResponse(
# #             short_summary=short_summary,
# #             detailed_summary=detailed_summary,
# #             issue_classification=issue_classification,
# #             processing_time=processing_time,
# #             timestamp=datetime.now().isoformat()
# #         )
        
# #     except json.JSONDecodeError:
# #         raise HTTPException(status_code=400, detail="Invalid JSON file")
# #     except Exception as e:
# #         raise HTTPException(status_code=500, detail=f"Processing error: {str(e)}")

# # @app.post("/process-manual", response_model=SummaryResponse)
# # async def process_manual_input(input_data: ManualIssueInput):
# #     try:
# #         start_time = datetime.now()
        
# #         # Prepare issue data in the format expected by original functions
# #         issue_data = {
# #             "Issue_Description": input_data.issue_description,
# #             "Latest Issue Comment (Ministry/State)": input_data.latest_comments,
# #             "Latest Meeting Minutes": input_data.meeting_minutes,
# #             "Historical_Comments": [],
# #             "Historical_Updates": [],
# #             "Historical_Actions": []
# #         }
        
# #         # Convert to list format as expected by original functions
# #         issues_list = [issue_data]
        
# #         # Generate summaries
# #         short_summary = generate_short_summary(issues_list)
# #         detailed_summary = generate_detailed_summary(issues_list)
# #         issue_classification = classify_project_issue(issues_list)
        
# #         end_time = datetime.now()
# #         processing_time = (end_time - start_time).total_seconds()
        
# #         return SummaryResponse(
# #             short_summary=short_summary,
# #             detailed_summary=detailed_summary,
# #             issue_classification=issue_classification,
# #             processing_time=processing_time,
# #             timestamp=datetime.now().isoformat()
# #         )
        
# #     except Exception as e:
# #         raise HTTPException(status_code=500, detail=f"Processing error: {str(e)}")

# # if __name__ == "__main__":
# #     uvicorn.run(app, host="0.0.0.0", port=8000)


# from fastapi import FastAPI
# import urllib3
# # Disable SSL warnings
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# from fastapi import FastAPI, HTTPException, UploadFile, File
# from fastapi.middleware.cors import CORSMiddleware
# from pydantic import BaseModel
# from typing import Optional, List, Dict
# import requests
# import json
# import uvicorn
# from datetime import datetime
# import urllib3

# # Disable SSL warnings
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# app = FastAPI(title="Gati Shakti Issue Summarizer API", version="1.0.0")

# # Enable CORS
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # Pydantic models
# class ManualIssueInput(BaseModel):
#     project_id: str
#     project_name: str
#     issue_description: str
#     latest_comments: Optional[str] = ""
#     meeting_minutes: Optional[str] = ""
#     responsible_authority: Optional[str] = ""

# class SummaryResponse(BaseModel):
#     short_summary: str
#     detailed_summary: str
#     issue_classification: str
#     processing_time: float
#     timestamp: str

# class StatsResponse(BaseModel):
#     total_projects: int = 1247
#     active_issues: int = 324
#     resolved_issues: int = 1893
#     summaries_generated: int = 5621

# def call_llm_api(prompt):
#     """LLM API call function with better error handling"""
#     url = "https://apis.airawat.cdac.in/negd/v1/chat/completions"
#     token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJ5YUtYUGtLZ1lKSUNqRTg3dHUwVnBUS3R5UUhxaUk1cyJ9.wJbzY_YRXGBug9sO1EYSwiCkRrI1bKnOL3uxSITxV94"
#     headers = {
#         "Authorization": f"Bearer {token}",
#         "Cookie": "SERVERID=api-manager",
#         "Content-Type": "application/json"
#     }

#     payload = {
#         "model": "meta/llama-3.2-11b-vision-instruct",
#         "messages": [{"role": "user", "content": prompt}],
#         "max_tokens": 2000
#     }
    
#     try:
        
#         response = requests.post(url, headers=headers, json=payload, verify=False, timeout=30)
#         response.raise_for_status()
#         result = response.json()
#         return result["choices"][0]["message"]["content"].strip()
#     except requests.Timeout:
#         raise HTTPException(status_code=503, detail="API is not responding (timeout). Please try again later.")
#     except requests.ConnectionError:
#         raise HTTPException(status_code=503, detail="Unable to connect to AI service. Please check your internet connection.")
#     except requests.HTTPError as e:
#         if e.response.status_code == 401:
#             raise HTTPException(status_code=503, detail="AI service authentication failed. Please contact administrator.")
#         elif e.response.status_code == 429:
#             raise HTTPException(status_code=503, detail="AI service is busy. Please try again in a few minutes.")
#         elif e.response.status_code >= 500:
#             raise HTTPException(status_code=503, detail="AI service is temporarily unavailable. Please try again later.")
#         else:
#             raise HTTPException(status_code=503, detail="AI service error. Please try again later.")
#     except (KeyError, IndexError, json.JSONDecodeError):
#         raise HTTPException(status_code=503, detail="Invalid response from AI service. Please try again.")
#     except Exception as e:
#         raise HTTPException(status_code=503, detail="AI service is not working. Please try again later.")

# def generate_short_summary(issue_data):
#     """Enhanced short summary using original prompt style"""
#     try:
#         prompt_template_short = f"""You are an expert summary generator. Your task is to read the input text and produce a clear, concise, and accurate short summary in just two or three lines based on the input text. 
# Focus on the main issue, what was discussed in the last meeting and what action has to be taken. Avoid redundancy, remove filler content, and ensure the output is easy to understand.
# Make sure to mention key stakeholders, deadlines, and current status if available.

# Input text: {issue_data}

# Provide short summary:"""
        
#         return call_llm_api(prompt_template_short)
#     except HTTPException:
#         raise
#     except Exception:
#         raise HTTPException(status_code=503, detail="AI service is not working. Please try again later.")

# def generate_detailed_summary(issue_data):
#     """Enhanced detailed summary using original prompt style"""
#     try:
#         prompt_template_detailed = f"""You are an expert project issue summarizer. Read the input text containing issue details such as issue title, description, last meeting minutes, updates, comments, and actions, and 
# produce a single clear, consolidated paragraph that captures all key and relevant information. The summary must be detailed descriptive, professional, and precise, with no redundancy, filler, or vague phrasing. 
# Do not create separate sections or headings; instead, merge all the issue details in the consolidated way into one seamless narrative and process flow such as:
# issue description, any updates or comments on that, anything decided in the last meeting and what are the actions taken etc.
# The output should be self-contained so the reader does not need to refer to the original text, and it should only include all the important, useful context without repeating information.
# Include the complete chain of events chronologically: what is the issue → what actions were taken → current status → meeting decisions → next steps → timeline.
# Mention all key stakeholders, amounts, dates, and dependencies clearly.

# Input text: {issue_data}

# Provide detailed summary:"""
        
#         return call_llm_api(prompt_template_detailed)
#     except HTTPException:
#         raise
#     except Exception:
#         raise HTTPException(status_code=503, detail="AI service is not working. Please try again later.")

# def classify_project_issue(issue_data):
#     """Detailed issue classification"""
#     try:
#         prompt_classification = f"""You are an expert project issue classifier for infrastructure and development projects. Based on the input data, classify the issue into ONE of the following 8 categories:

# **CLASSIFICATION CATEGORIES:**

# 1. **LAND & ACQUISITION ISSUES**
#    - Scope: Land acquisition delays, encroachment removal, possession handover, compensation disputes
#    - Sub-types: Land Acquisition, Land Encroachment, Demand High Compensation
#    - Keywords: land acquisition, compensation, possession, encroachment, demarcation, revenue records
#    - Example: "Demarcation of land and removal of encroachment pending on the land along railway line"

# 2. **REGULATORY APPROVALS & PERMISSIONS**
#    - Scope: Pending government approvals, NOCs, environmental clearances, statutory permissions
#    - Sub-types: Approval pending, Lease pending, RoW (Right of Way) Permission
#    - Keywords: approval, NOC, clearance, permission, license, statutory, regulatory
#    - Example: "Pending permission/NOC by DUSIB for relocation of shops/structures"

# 3. **UTILITY & INFRASTRUCTURE COORDINATION**
#    - Scope: Electricity connections, utility shifting, water resources, coordination with utility providers
#    - Sub-types: Electricity issue, Water resources issue, Waiver of Maintenance charges
#    - Keywords: utility shifting, electrical, power lines, water, telecom, infrastructure coordination
#    - Example: "Shifting of electrical poles and lines going through AIIMS Rewari campus"

# 4. **ROADS & CONNECTIVITY**
#    - Scope: Access roads, connectivity infrastructure, transportation links
#    - Sub-types: Roads Construction & Connectivity
#    - Keywords: access road, connectivity, transportation, highway, bridge, road construction
#    - Example: "Requirement of an access road for movement of heavy vehicle and equipment"

# 5. **CONSTRUCTION & EXECUTION DELAYS**
#    - Scope: Construction delays, contractor issues, execution bottlenecks
#    - Sub-types: Construction delay
#    - Keywords: construction delay, contractor, execution, timeline, project delay, building
#    - Example: Issues related to project timeline delays and execution challenges

# 6. **FINANCIAL & DISBURSEMENT**
#    - Scope: Payment delays, fund disbursement issues, financial approvals
#    - Sub-types: Disbursement Issue
#    - Keywords: payment, disbursement, funds, financial, budget, cost, money transfer
#    - Example: Financial processing and payment-related bottlenecks

# 7. **ENVIRONMENTAL & COMPLIANCE**
#    - Scope: Environmental clearances, forest permissions, compliance issues
#    - Sub-types: Environmental approvals, wildlife clearances
#    - Keywords: environmental clearance, forest, wildlife, ecology, compliance, green clearance
#    - Example: Environmental and regulatory compliance matters

# 8. **STAKEHOLDER COORDINATION**
#    - Scope: Multi-agency coordination, inter-ministerial issues, stakeholder alignment
#    - Sub-types: Cross-departmental coordination issues
#    - Keywords: coordination, inter-ministerial, multi-agency, stakeholder, departmental alignment
#    - Example: Issues requiring coordination between multiple government bodies

# **INSTRUCTIONS:**
# - Analyze the issue description, comments, meeting minutes, and actions carefully
# - Identify the primary bottleneck or challenge that is causing the delay
# - Select the MOST APPROPRIATE single category based on the core issue
# - Consider the main stakeholders involved and the type of resolution required
# - Provide ONLY the exact category name as output (e.g., "UTILITY & INFRASTRUCTURE COORDINATION")

# **INPUT DATA:** {issue_data}

# **CLASSIFICATION:**"""
        
#         return call_llm_api(prompt_classification)
#     except HTTPException:
#         raise
#     except Exception:
#         raise HTTPException(status_code=503, detail="AI service is not working. Please try again later.")

# def extract_issues(data, selected_keys):
#     """Extract issues from JSON data"""
#     if not data or "Project" not in data or "Issues" not in data["Project"]:
#         raise HTTPException(status_code=400, detail="Invalid JSON structure")
    
#     issues = data["Project"]["Issues"]
#     if not issues:
#         raise HTTPException(status_code=400, detail="No issues found in JSON")
    
#     issue_subdict = [
#         {k: issue[k] for k in selected_keys if k in issue}
#         for issue in issues
#     ]
#     return issue_subdict

# @app.get("/")
# async def root():
#     return {"message": "Gati Shakti Issue Summarizer API is running"}

# @app.get("/stats", response_model=StatsResponse)
# async def get_stats():
#     return StatsResponse()

# @app.post("/process-json", response_model=SummaryResponse)
# async def process_json_file(file: UploadFile = File(...)):
#     if not file.filename.endswith('.json'):
#         raise HTTPException(status_code=400, detail="Only JSON files are allowed")
    
#     try:
#         start_time = datetime.now()
        
#         # Read JSON file
#         content = await file.read()
#         json_data = json.loads(content.decode('utf-8'))
        
#         # Selected keys from original script
#         selected_keys = [
#             "Issue_Description",
#             "Latest Issue Comment (Ministry/State)",
#             "Latest Meeting Minutes",
#             "Historical_Comments",
#             "Historical_Updates",
#             "Historical_Actions"
#         ]
        
#         # Extract issues
#         issues_subset = extract_issues(json_data, selected_keys)
        
#         # Generate summaries using original approach
#         short_summary = generate_short_summary(issues_subset)
#         detailed_summary = generate_detailed_summary(issues_subset)
#         issue_classification = classify_project_issue(issues_subset)
        
#         end_time = datetime.now()
#         processing_time = (end_time - start_time).total_seconds()
        
#         return SummaryResponse(
#             short_summary=short_summary,
#             detailed_summary=detailed_summary,
#             issue_classification=issue_classification,
#             processing_time=processing_time,
#             timestamp=datetime.now().isoformat()
#         )
        
#     except json.JSONDecodeError:
#         raise HTTPException(status_code=400, detail="Invalid JSON file format")
#     except HTTPException:
#         raise
#     except Exception as e:
#         print(f"Unexpected error: {e}")  # Log for debugging
#         raise HTTPException(status_code=503, detail="AI service is not working. Please try again later.")

# @app.post("/process-manual", response_model=SummaryResponse)
# async def process_manual_input(input_data: ManualIssueInput):
#     try:
#         start_time = datetime.now()
        
#         # Prepare issue data in the format expected by original functions
#         issue_data = {
#             "Issue_Description": input_data.issue_description,
#             "Latest Issue Comment (Ministry/State)": input_data.latest_comments,
#             "Latest Meeting Minutes": input_data.meeting_minutes,
#             "Historical_Comments": [],
#             "Historical_Updates": [],
#             "Historical_Actions": []
#         }
        
#         # Convert to list format as expected by original functions
#         issues_list = [issue_data]
        
#         # Generate summaries
#         short_summary = generate_short_summary(issues_list)
#         detailed_summary = generate_detailed_summary(issues_list)
#         issue_classification = classify_project_issue(issues_list)
        
#         end_time = datetime.now()
#         processing_time = (end_time - start_time).total_seconds()
        
#         return SummaryResponse(
#             short_summary=short_summary,
#             detailed_summary=detailed_summary,
#             issue_classification=issue_classification,
#             processing_time=processing_time,
#             timestamp=datetime.now().isoformat()
#         )
        
#     except HTTPException:
#         raise
#     except Exception as e:
#         print(f"Unexpected error: {e}")  # Log for debugging
#         raise HTTPException(status_code=503, detail="AI service is not working. Please try again later.")

# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000)

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import json
import uvicorn
import os
from datetime import datetime
import logging
from similar_issues2 import GatiShaktiSimilarIssuesRAG
from model import ManualIssueInput, EnhancedSummaryResponse, StatsResponse

app = FastAPI(title="Gati Shakti Issue Summarizer API", version="2.0.0")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize RAG system globally
rag_system = GatiShaktiSimilarIssuesRAG(use_llm=True)

sample_issues_db = []
database_embeddings = None


import re
def clean_llm_response(text: str) -> str:
    """
    Cleans up special characters, formatting symbols, and unnecessary whitespace
    from the LLM response text.


    Args:
    text (str): The raw response text from the LLM.


    Returns:
    str: The cleaned and formatted text.
    """
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

def load_database_on_startup():
    global sample_issues_db, database_embeddings
    database_file_path = 'data/prod/gati_shakti_feature_engineering.json'
    try:
        if os.path.exists(database_file_path):
            with open(database_file_path, 'r', encoding='utf-8') as f:
                sample_issues_db = json.load(f)
            if sample_issues_db:
                database_embeddings = rag_system.create_issue_embeddings(sample_issues_db)
            logger.info(f"Database loaded with {len(sample_issues_db)} issues")
    except Exception as e:
        logger.error(f"Failed to load database: {e}")

load_database_on_startup()

@app.get("/")
def root():
    return {"message": "Gati Shakti Issue Summarizer API with RAG is running"}

@app.get("/stats", response_model=StatsResponse)
def get_stats():
    return StatsResponse()

@app.get("/database-status")
def database_status():
    global database_embeddings
    embeddings_ready = database_embeddings is not None
    return {
        "database_loaded": len(sample_issues_db) > 0,
        "embeddings_ready": embeddings_ready,
        "total_issues": len(sample_issues_db),
        "status": "Ready" if embeddings_ready else "Loading..."
    }

@app.post("/process-issue", response_model=EnhancedSummaryResponse)
async def process_issue(file: UploadFile = File(...)):
    if not file.filename.endswith('.json'):
        raise HTTPException(status_code=400, detail="Only JSON files are allowed")

    content = await file.read()
    issue_data = json.loads(content.decode('utf-8'))

    issue_text = json.dumps({
        "Issue_Description": issue_data.get("issueDescription", ""),
        "Latest Issue Comment (Ministry/State)": issue_data.get("latestIssueCommentMinistryOrState", ""),
        "Latest Meeting Minutes": issue_data.get("latestMeetingDecision", ""),
        "Historical_Comments": [c.get("comment", "") for c in issue_data.get("comments", [])],
        "Historical_Actions": [d.get("meetingDecision", "") for d in issue_data.get("decisions", [])]
    })


    short_summary_prompt = f"""You are an expert summary generator. Your task is to read the input text and produce a clear, concise, and accurate short summary in just two or three lines based on the input text. 
    Focus on the main issue, what was discussed in the last meeting and what action has to be taken. Avoid redundancy, remove filler content, and ensure the output is easy to understand.
    Make sure to mention key stakeholders, deadlines, and current status if available.

    Input text: {issue_text}

    Provide short summary:"""


    long_summary_prompt = f"""You are an expert project issue summarizer. Read the input text containing issue details such as issue title, description, last meeting minutes, updates, comments, and actions, and 
    produce a single clear, consolidated paragraph that captures all key and relevant information. The summary must be detailed descriptive, professional, and precise, with no redundancy, filler, or vague phrasing. 
    Do not create separate sections or headings; instead, merge all the issue details in the consolidated way into one seamless narrative and process flow such as:
    issue description, any updates or comments on that, anything decided in the last meeting and what are the actions taken etc.
    The output should be self-contained so the reader does not need to refer to the original text, and it should only include all the important, useful context without repeating information.
    Include the complete chain of events chronologically: what is the issue → what actions were taken → current status → meeting decisions → next steps → timeline.
    Mention all key stakeholders, amounts, dates, and dependencies clearly.

    Input text: {issue_text}

    Provide detailed summary:"""

    issue_prompt_classification = f"""You are an expert project issue classifier for infrastructure and development projects. Based on the input data, classify the issue into ONE of the following 8 categories:

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
    

    short_summary = rag_system._call_llm_api(short_summary_prompt)
    detailed_summary = rag_system._call_llm_api(long_summary_prompt)
    issue_classification = rag_system._call_llm_api(issue_prompt_classification)

    similar_issues_raw = rag_system.find_similar_issues(
        query_issue=issue_data,
        all_issues=sample_issues_db,
        all_embeddings=database_embeddings,
        query_idx=0,
        k=3
    )

    similar_issues = [{
        "issue_key": sim['issue'].get('issue_key', 'unknown'),
        "title": sim['issue'].get('issueTitle', 'No title'),
        "sector": sim['issue'].get('feat_sector', 'Unknown'),
        "issue_category": sim['issue'].get('feat_issue_category', 'Unknown'),
        "similarity_score": sim['combined_similarity'],
        "similarity_reasons": sim['match_reasons'],
        "llm_analysis": clean_llm_response(sim['llm_analysis']),
        "manual_issue_type": sim['issue'].get('feat_manual_issue_type', 'Unknown'),
        "resolution_strategy": sim['issue'].get('feat_manual_resolution_strategy', 'Unknown')
    } for sim in similar_issues_raw]

    return EnhancedSummaryResponse(
        short_summary=short_summary,
        detailed_summary=detailed_summary,
        issue_classification=issue_classification,
        similar_issues=similar_issues,
        processing_time=(datetime.now() - datetime.utcnow()).total_seconds(),
        timestamp=datetime.now().isoformat()
    )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
