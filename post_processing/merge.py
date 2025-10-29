"""
PMG Issue AI Pipeline - Post Processing Module
Combines all ML module outputs into final Excel with exact required columns
"""

import os
import sys
import json
import logging
import traceback
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config.settings import get_settings

logger = logging.getLogger(__name__)


class PostProcessingError(Exception):
    """Custom exception for post processing errors"""
    pass


class PostProcessingPipeline:
    """
    Main class for combining all ML module outputs into final Excel
    Creates the exact columns required for the database and frontend
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize the post processing pipeline
        
        Args:
            config_path: Path to configuration file
        """
        # Load configuration
        self.settings = get_settings(config_path)
        
        # Setup logging
        self._setup_logging()
        
        # Data paths - all input files from previous modules
        self.all_issues_features_file = self.settings.get('data_paths.all_issues_features_file')
        print(self.all_issues_features_file)
        self.description_output_file = self.settings.get('data_paths.descriptions_file')
        self.classification_output_file = self.settings.get('data_paths.classifications_file')
        self.similar_issues_output_file = self.settings.get('data_paths.similar_issues_file')
        self.timeline_predictions_output_file = self.settings.get('data_paths.timeline_predictions_file')
        self.action_recommendations_output_file = self.settings.get('data_paths.action_recommendations_file')
        
        # Output files
        self.final_excel_file = self.settings.get('data_paths.final_excel_file', 'data/outputs/final_ml_results.xlsx')
        self.final_json_file = self.settings.get('data_paths.final_json_file', 'data/outputs/final_ml_results.json')
        
        self.logger.info("PostProcessingPipeline initialized successfully")
    
    def _setup_logging(self) -> None:
        """Setup logging configuration"""
        log_config = self.settings.get_logging_config()
        log_dir = log_config.get('logs_path', 'logs/')
        
        # Create logs directory
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        
        # Setup file handler
        log_file = Path(log_dir) / "post_processing.log"
        
        # Configure logging
        logging.basicConfig(
            level=getattr(logging, log_config.get('level', 'INFO')),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler() if log_config.get('enable_console_logging', True) else logging.NullHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Logging initialized. Log file: {log_file}")
    
    def _validate_inputs(self) -> None:
        """
        Validate that all required input files exist
        
        Raises:
            PostProcessingError: If required inputs are missing
        """
        self.logger.info("Validating input requirements...")
        
        required_files = {
            'All Issues Features': self.all_issues_features_file,
            'Description Output': self.description_output_file,
            'Classification Output': self.classification_output_file,
            'Similar Issues Output': self.similar_issues_output_file,
            'Timeline Predictions Output': self.timeline_predictions_output_file,
            'Action Recommendations Output': self.action_recommendations_output_file
        }
        
        missing_files = []
        for name, file_path in required_files.items():
            print(file_path)
            if not os.path.exists(file_path):
                missing_files.append(f"{name}: {file_path}")
        
        if missing_files:
            raise PostProcessingError(
                f"Required input files not found:\n" + "\n".join(missing_files) + 
                "\nPlease run all previous pipeline stages first."
            )
        
        # Validate files are not empty
        for name, file_path in required_files.items():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if not data:
                        raise PostProcessingError(f"{name} file is empty: {file_path}")
                    
                    self.logger.info(f"Validated {name}: {len(data)} records")
                    
            except json.JSONDecodeError:
                raise PostProcessingError(f"{name} file contains invalid JSON: {file_path}")
            except Exception as e:
                raise PostProcessingError(f"Error reading {name} file: {e}")
        
        self.logger.info("Input validation successful")
    
    def _load_all_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Load all data files from previous modules
        
        Returns:
            Dictionary containing all loaded data
            
        Raises:
            PostProcessingError: If loading fails
        """
        self.logger.info("Loading all data files...")
        
        try:
            # Load all issues features (base data)
            with open(self.all_issues_features_file, 'r', encoding='utf-8') as f:
                all_issues = json.load(f)
            self.logger.info(f"Loaded {len(all_issues)} all issues")
            
            # Load description outputs
            with open(self.description_output_file, 'r', encoding='utf-8') as f:
                descriptions = json.load(f)
            self.logger.info(f"Loaded {len(descriptions)} descriptions")
            
            # Load classification outputs
            with open(self.classification_output_file, 'r', encoding='utf-8') as f:
                classifications = json.load(f)
            self.logger.info(f"Loaded {len(classifications)} classifications")
            
            # Load similar issues outputs
            with open(self.similar_issues_output_file, 'r', encoding='utf-8') as f:
                similar_issues = json.load(f)
            self.logger.info(f"Loaded {len(similar_issues)} similar issues results")
            
            # Load timeline predictions
            with open(self.timeline_predictions_output_file, 'r', encoding='utf-8') as f:
                timeline_predictions = json.load(f)
            self.logger.info(f"Loaded {len(timeline_predictions)} timeline predictions")
            
            # Load action recommendations
            with open(self.action_recommendations_output_file, 'r', encoding='utf-8') as f:
                action_recommendations = json.load(f)
            self.logger.info(f"Loaded {len(action_recommendations)} action recommendations")
            
            return {
                'all_issues': all_issues,
                'descriptions': descriptions,
                'classifications': classifications,
                'similar_issues': similar_issues,
                'timeline_predictions': timeline_predictions,
                'action_recommendations': action_recommendations
            }
            
        except Exception as e:
            raise PostProcessingError(f"Failed to load data: {e}")
    
    def _create_lookups(self, data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
        """
        Create lookup dictionaries for fast access to all module outputs
        
        Args:
            data: Dictionary containing all loaded data
            
        Returns:
            Dictionary with lookup tables
        """
        self.logger.info("Creating lookup dictionaries...")
        
        # Descriptions lookup
        descriptions_lookup = {}
        for desc in data['descriptions']:
            project_id = desc.get('projectId', '')
            issue_id = desc.get('issueId', '')
            if project_id and issue_id:
                key = f"{project_id}_{issue_id}"
                descriptions_lookup[key] = desc
        
        # Classifications lookup
        classifications_lookup = {}
        for cls in data['classifications']:
            project_id = cls.get('projectId', '')
            issue_id = cls.get('issueId', '')
            if project_id and issue_id:
                key = f"{project_id}_{issue_id}"
                classifications_lookup[key] = cls
        
        # Similar issues lookup
        similar_issues_lookup = {}
        for sim in data['similar_issues']:
            project_id = sim.get('projectId', '')
            issue_id = sim.get('issueId', '')
            if project_id and issue_id:
                key = f"{project_id}_{issue_id}"
                similar_issues_lookup[key] = sim
        
        # Timeline predictions lookup
        timeline_lookup = {}
        for tl in data['timeline_predictions']:
            project_id = tl.get('projectId', '')
            issue_id = tl.get('issueId', '')
            if project_id and issue_id:
                key = f"{project_id}_{issue_id}"
                timeline_lookup[key] = tl
        
        # Action recommendations lookup
        actions_lookup = {}
        for act in data['action_recommendations']:
            project_id = act.get('projectId', '')
            issue_id = act.get('issueId', '')
            if project_id and issue_id:
                key = f"{project_id}_{issue_id}"
                actions_lookup[key] = act
        
        self.logger.info(f"Created lookups: {len(descriptions_lookup)} descriptions, "
                        f"{len(classifications_lookup)} classifications, "
                        f"{len(similar_issues_lookup)} similar issues, "
                        f"{len(timeline_lookup)} timeline predictions, "
                        f"{len(actions_lookup)} action recommendations")
        
        return {
            'descriptions': descriptions_lookup,
            'classifications': classifications_lookup,
            'similar_issues': similar_issues_lookup,
            'timeline_predictions': timeline_lookup,
            'action_recommendations': actions_lookup
        }
    
    def _combine_single_issue(self, issue: Dict[str, Any], lookups: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Combine all ML outputs for a single issue into final format
        
        Args:
            issue: Base issue data
            lookups: Lookup dictionaries for all module outputs
            
        Returns:
            Dictionary with combined results in exact required format
        """
        project_id = issue.get('projectId', '')
        issue_id = issue.get('issueId', '')
        issue_key = f"{project_id}_{issue_id}"
        
        # Get data from all modules
        desc_data = lookups['descriptions'].get(issue_key, {})
        cls_data = lookups['classifications'].get(issue_key, {})
        sim_data = lookups['similar_issues'].get(issue_key, {})
        timeline_data = lookups['timeline_predictions'].get(issue_key, {})
        action_data = lookups['action_recommendations'].get(issue_key, {})
        
        # Create final record with exact required columns
        final_record = {
            # Basic identification
            'Project ID': project_id,
            'Issue ID': issue_id,
            
            # Descriptions (from description module)
            'Short Description': desc_data.get('shortDescription', ''),
            'Long Description': desc_data.get('longDescription', ''),
            
            # Classification (from classification module)
            'Project Issue class': cls_data.get('classification', ''),
            
            # Similar Issues (from similar issues module)
            'Similar Issue 1 Project ID': sim_data.get('similarIssue1ProjectId', ''),
            'Similar Issue 1 Issue ID': sim_data.get('similarIssue1IssueId', ''),
            'Similar Issue 1 Name': sim_data.get('similarIssue1Name', ''),
            'Similar Issue 1 Start Date': sim_data.get('similarIssue1StartDate', ''),
            'Similar Issue 1 Resolution Time': sim_data.get('similarIssue1ResolutionTime', ''),
            
            'Similar Issue 2 Project ID': sim_data.get('similarIssue2ProjectId', ''),
            'Similar Issue 2 Issue ID': sim_data.get('similarIssue2IssueId', ''),
            'Similar Issue 2 Name': sim_data.get('similarIssue2Name', ''),
            'Similar Issue 2 Start Date': sim_data.get('similarIssue2StartDate', ''),
            'Similar Issue 2 Resolution Time': sim_data.get('similarIssue2ResolutionTime', ''),
            
            'Similar Issue 3 Project ID': sim_data.get('similarIssue3ProjectId', ''),
            'Similar Issue 3 Issue ID': sim_data.get('similarIssue3IssueId', ''),
            'Similar Issue 3 Name': sim_data.get('similarIssue3Name', ''),
            'Similar Issue 3 Start Date': sim_data.get('similarIssue3StartDate', ''),
            'Similar Issue 3 Resolution Time': sim_data.get('similarIssue3ResolutionTime', ''),
            
            # Timeline Predictions (from timeline module)
            'Time AI Predicted Timeline': timeline_data.get('timeAiPredictedTimeline', ''),
            'Issue Start Date': timeline_data.get('issueStartDate', issue.get('issueCreationDate', '')),
            'Predicted End Date': timeline_data.get('predictedEndDate', ''),
            'Timeline Resolution Rationale': timeline_data.get('timelineReasoning', ''),
            
            # Action Recommendations (from action recommendation module)
            'Immediate next steps': action_data.get('immediateNextSteps', ''),
            'Learn from similar Issues': action_data.get('learnFromSimilarIssues', ''),
            'Strategic Best practice': action_data.get('strategicBestPractice', '')
        }
        
        return final_record
    
    def _process_all_issues(self, all_issues: List[Dict[str, Any]], lookups: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process all issues to combine ML outputs
        
        Args:
            all_issues: List of all issues
            lookups: Lookup dictionaries for all module outputs
            
        Returns:
            List of combined results
        """
        self.logger.info(f"Processing {len(all_issues)} issues for final combination...")
        
        final_results = []
        successful_count = 0
        error_count = 0
        
        for i, issue in enumerate(all_issues, 1):
            try:
                self.logger.info(f"Processing issue {i}/{len(all_issues)}")
                
                combined_result = self._combine_single_issue(issue, lookups)
                final_results.append(combined_result)
                successful_count += 1
                
                # Log progress every 100 issues
                if i % 100 == 0:
                    self.logger.info(f"Progress: {i}/{len(all_issues)} issues processed "
                                   f"(Success: {successful_count}, Errors: {error_count})")
                
            except Exception as e:
                self.logger.error(f"Error processing issue {i}: {e}")
                error_count += 1
                
                # Add error result to maintain consistency
                final_results.append({
                    'Project ID': issue.get('projectId', 'Unknown'),
                    'Issue ID': issue.get('issueId', f'Issue_{i}'),
                    'Short Description': f'Processing error: {str(e)}',
                    'Long Description': 'Please contact support for assistance',
                    'Project Issue class': 'Error',
                    'Similar Issue 1 Project ID': '',
                    'Similar Issue 1 Issue ID': '',
                    'Similar Issue 1 Name': '',
                    'Similar Issue 1 Start Date': '',
                    'Similar Issue 1 Resolution Time': '',
                    'Similar Issue 2 Project ID': '',
                    'Similar Issue 2 Issue ID': '',
                    'Similar Issue 2 Name': '',
                    'Similar Issue 2 Start Date': '',
                    'Similar Issue 2 Resolution Time': '',
                    'Similar Issue 3 Project ID': '',
                    'Similar Issue 3 Issue ID': '',
                    'Similar Issue 3 Name': '',
                    'Similar Issue 3 Start Date': '',
                    'Similar Issue 3 Resolution Time': '',
                    'Time AI Predicted Timeline': '',
                    'Issue Start Date': issue.get('issueCreationDate', ''),
                    'Predicted End Date': '',
                    'Timeline Resolution Rationale': 'Processing error occurred',
                    'Immediate next steps': 'Contact support for assistance',
                    'Learn from similar Issues': 'Review issue data and retry',
                    'Strategic Best practice': 'Check system logs and retry processing'
                })
        
        self.logger.info(f"Processing completed: {successful_count} successful, {error_count} errors")
        return final_results
    
    def _save_excel_output(self, final_results: List[Dict[str, Any]]) -> str:
        """
        Save final results to Excel file
        
        Args:
            final_results: List of combined results
            
        Returns:
            Path to output Excel file
            
        Raises:
            PostProcessingError: If saving fails
        """
        self.logger.info("Saving results to Excel...")
        
        try:
            # Create output directory
            Path(self.final_excel_file).parent.mkdir(parents=True, exist_ok=True)
            
            # Create DataFrame
            df = pd.DataFrame(final_results)
            
            # Ensure exact column order as specified
            required_columns = [
                'Project ID', 'Issue ID', 'Short Description', 'Long Description', 'Project Issue class',
                'Similar Issue 1 Project ID', 'Similar Issue 1 Issue ID', 'Similar Issue 1 Name', 
                'Similar Issue 1 Start Date', 'Similar Issue 1 Resolution Time',
                'Similar Issue 2 Project ID', 'Similar Issue 2 Issue ID', 'Similar Issue 2 Name', 
                'Similar Issue 2 Start Date', 'Similar Issue 2 Resolution Time',
                'Similar Issue 3 Project ID', 'Similar Issue 3 Issue ID', 'Similar Issue 3 Name', 
                'Similar Issue 3 Start Date', 'Similar Issue 3 Resolution Time',
                'Time AI Predicted Timeline', 'Issue Start Date', 'Predicted End Date', 'Timeline Resolution Rationale',
                'Immediate next steps', 'Learn from similar Issues', 'Strategic Best practice'
            ]
            
            # Reorder columns
            df = df[required_columns]
            
            # Save to Excel with formatting
            with pd.ExcelWriter(self.final_excel_file, engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name='ML_Results', index=False)
                
                # Get workbook and worksheet
                workbook = writer.book
                worksheet = writer.sheets['ML_Results']
                
                # Add formatting
                header_format = workbook.add_format({
                    'bold': True,
                    'text_wrap': True,
                    'valign': 'top',
                    'fg_color': '#D7E4BC',
                    'border': 1
                })
                
                # Format headers
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                
                # Auto-adjust column widths
                for i, col in enumerate(df.columns):
                    column_len = max(df[col].astype(str).str.len().max(), len(col)) + 2
                    worksheet.set_column(i, i, min(column_len, 50))  # Max width 50
            
            self.logger.info(f"Excel file saved successfully: {self.final_excel_file}")
            return self.final_excel_file
            
        except Exception as e:
            self.logger.error(f"Error saving Excel file: {e}")
            raise PostProcessingError(f"Failed to save Excel file: {e}")
    
    def _save_json_output(self, final_results: List[Dict[str, Any]]) -> str:
        """
        Save final results to JSON file (for database import)
        
        Args:
            final_results: List of combined results
            
        Returns:
            Path to output JSON file
            
        Raises:
            PostProcessingError: If saving fails
        """
        self.logger.info("Saving results to JSON...")
        
        try:
            # Create output directory
            Path(self.final_json_file).parent.mkdir(parents=True, exist_ok=True)
            
            # Save JSON with metadata
            output_data = {
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'total_records': len(final_results),
                    'pipeline_version': '1.0.0',
                    'description': 'Combined ML pipeline results for PMG Issue AI system'
                },
                'data': final_results
            }
            
            with open(self.final_json_file, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"JSON file saved successfully: {self.final_json_file}")
            return self.final_json_file
            
        except Exception as e:
            self.logger.error(f"Error saving JSON file: {e}")
            raise PostProcessingError(f"Failed to save JSON file: {e}")
    
    def _generate_summary_report(self, final_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate summary report of processing results
        
        Args:
            final_results: List of combined results
            
        Returns:
            Dictionary with summary statistics
        """
        df = pd.DataFrame(final_results)
        
        # Calculate statistics
        total_issues = len(df)
        issues_with_descriptions = len(df[df['Short Description'].str.len() > 0])
        issues_with_classifications = len(df[df['Project Issue class'].str.len() > 0])
        issues_with_similar = len(df[df['Similar Issue 1 Name'].str.len() > 0])
        issues_with_timeline = len(df[df['Time AI Predicted Timeline'].str.len() > 0])
        issues_with_actions = len(df[df['Immediate next steps'].str.len() > 0])
        
        # Error counts
        error_descriptions = len(df[df['Short Description'].str.contains('error', case=False, na=False)])
        error_classifications = len(df[df['Project Issue class'] == 'Error'])
        
        summary = {
            'total_issues': total_issues,
            'completion_rates': {
                'descriptions': f"{issues_with_descriptions}/{total_issues} ({issues_with_descriptions/total_issues*100:.1f}%)",
                'classifications': f"{issues_with_classifications}/{total_issues} ({issues_with_classifications/total_issues*100:.1f}%)",
                'similar_issues': f"{issues_with_similar}/{total_issues} ({issues_with_similar/total_issues*100:.1f}%)",
                'timeline_predictions': f"{issues_with_timeline}/{total_issues} ({issues_with_timeline/total_issues*100:.1f}%)",
                'action_recommendations': f"{issues_with_actions}/{total_issues} ({issues_with_actions/total_issues*100:.1f}%)"
            },
            'error_counts': {
                'description_errors': error_descriptions,
                'classification_errors': error_classifications
            }
        }
        
        return summary
    
    def run(self) -> Dict[str, Any]:
        """
        Run the complete post processing pipeline
        
        Returns:
            Dictionary containing execution results and output paths
            
        Raises:
            PostProcessingError: If any step fails
        """
        start_time = datetime.now()
        self.logger.info("="*80)
        self.logger.info("STARTING PMG ISSUE AI PIPELINE - POST PROCESSING")
        self.logger.info("="*80)
        
        try:
            # Step 1: Validate inputs
            self._validate_inputs()
            
            # Step 2: Load all data
            data = self._load_all_data()
            
            # Step 3: Create lookups
            lookups = self._create_lookups(data)
            
            # Step 4: Process all issues
            final_results = self._process_all_issues(data['all_issues'], lookups)
            
            # Step 5: Save outputs
            excel_file = self._save_excel_output(final_results)
            json_file = self._save_json_output(final_results)
            
            
            # Calculate execution time
            execution_time = datetime.now() - start_time
            
            # Prepare results
            results = {
                "status": "success",
                "execution_time": str(execution_time),
                "total_issues": len(data['all_issues']),
                "final_records": len(final_results),
                "output_files": {
                    "excel_file": excel_file,
                    "json_file": json_file
                },
                # "summary": summary
            }
            
            self.logger.info("="*80)
            self.logger.info("POST PROCESSING COMPLETED SUCCESSFULLY")
            self.logger.info(f"Total issues processed: {len(data['all_issues'])}")
            self.logger.info(f"Final records created: {len(final_results)}")
            self.logger.info(f"Excel file: {excel_file}")
            self.logger.info(f"JSON file: {json_file}")
            self.logger.info(f"Execution time: {execution_time}")
            self.logger.info("="*80)
            
            return results
            
        except Exception as e:
            execution_time = datetime.now() - start_time
            error_msg = f"Post processing failed after {execution_time}: {e}"
            self.logger.error(error_msg)
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            
            raise PostProcessingError(error_msg)


def main():
    """
    Main function to run post processing
    Can be called independently or as part of the pipeline
    """
    try:
        pipeline = PostProcessingPipeline()
        results = pipeline.run()
        
        print("\nPost Processing Results:")
        print("="*50)
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())