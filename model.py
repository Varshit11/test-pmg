from pydantic import BaseModel
from typing import Optional, List

class ManualIssueInput(BaseModel):
    project_id: str
    project_name: Optional[str] = None
    issue_description: str
    latest_comments: Optional[str] = ""
    meeting_minutes: Optional[str] = ""
    responsible_authority: Optional[str] = ""

class SimilarIssue(BaseModel):
    issue_key: str
    title: str
    sector: str
    issue_category: str
    similarity_score: float
    similarity_reasons: List[str]
    llm_analysis: str
    manual_issue_type: str
    resolution_strategy: str

class EnhancedSummaryResponse(BaseModel):
    short_summary: str
    detailed_summary: str
    issue_classification: str
    similar_issues: List[SimilarIssue]
    processing_time: float
    timestamp: str

class StatsResponse(BaseModel):
    total_projects: int = 1247
    active_issues: int = 324
    resolved_issues: int = 1893
    summaries_generated: int = 5621