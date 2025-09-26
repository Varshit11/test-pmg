# import json
# import pandas as pd
# import numpy as np
# from typing import Dict, List, Any, Tuple
# import logging
# from datetime import datetime
# import requests
# from sentence_transformers import SentenceTransformer
# from sklearn.metrics.pairwise import cosine_similarity
# from sklearn.preprocessing import StandardScaler
# import warnings
# warnings.filterwarnings('ignore')

# # Set up logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# class GatiShaktiSimilarIssuesRAG:
#     """
#     Enhanced RAG pipeline for finding similar issues with comprehensive reasoning
#     """
    
#     def __init__(self, use_llm: bool = True):
#         self.use_llm = use_llm
        
#         # LLM Configuration
#         self.llm_url = "https://apis.airawat.cdac.in/negd/v1/chat/completions"
#         self.llm_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJ5YUtYUGtLZ1lKSUNqRTg3dHUwVnBUS3R5UUhxaUk1cyJ9.wJbzY_YRXGBug9sO1EYSwiCkRrI1bKnOL3uxSITxV94"
        
#         # Initialize embedding model
#         self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        
#         # Key features for similarity analysis
#         self.manual_features = [
#             'feat_manual_issue_type',
#             'feat_manual_stakeholder_complexity',
#             'feat_manual_resolution_strategy', 
#             'feat_manual_timeline_criticality',
#             'feat_manual_geographic_scope',
#             'feat_manual_bureaucratic_complexity',
#             'feat_manual_environmental_impact',
#             'feat_manual_financial_complexity',
#             'feat_manual_seasonal_dependency',
#             'feat_manual_public_sensitivity',
#             'feat_manual_legal_risk',
#             'feat_manual_tech_dependency',
#             'feat_manual_infrastructure_type',
#             'feat_manual_displacement_scale'
#         ]
        
#         self.clustering_features = [
#             'feat_clustering_issue_pattern',
#             'feat_clustering_root_cause',
#             'feat_clustering_solution_approach',
#             'feat_clustering_recurrence_pattern',
#             'feat_clustering_stakeholder_pattern',
#             'feat_clustering_communication_urgency',
#             'feat_clustering_evolution_stage'
#         ]
        
#         self.traditional_categorical = [
#             'feat_issue_category',
#             'feat_sector',
#             'feat_region',
#             'feat_project_stage',
#             'feat_responsible_authority'
#         ]
        
#         self.numerical_features = [
#             'feat_project_cost',
#             'feat_time_overrun_percent',
#             'feat_delay_risk_score',
#             'feat_urgency_score',
#             'feat_stakeholder_complexity',
#             'feat_physical_progress',
#             'feat_financial_progress'
#         ]
        
#         # Weights for different similarity components
#         self.similarity_weights = {
#             'text_similarity': 0.4,
#             'manual_features': 0.3,
#             'clustering_features': 0.2,
#             'numerical_features': 0.1
#         }

#     def create_issue_embeddings(self, issues_data: List[Dict[str, Any]]) -> np.ndarray:
#         """Create comprehensive text embeddings for all issues"""
#         logger.info("Creating text embeddings for all issues...")
        
#         texts = []
#         for issue in issues_data:
#             # Combine multiple text fields with weights
#             text_parts = []
            
#             # Primary text fields (high weight)
#             combined_text = issue.get('combined_text', '')
#             if combined_text:
#                 text_parts.extend([combined_text] * 3)
            
#             title = issue.get('issueTitle', '')
#             if title:
#                 text_parts.extend([title] * 2)
                
#             description = issue.get('issueDescription', '')
#             if description:
#                 text_parts.extend([description] * 2)
            
#             # Feature-based text (medium weight)
#             for feat in self.manual_features + self.clustering_features:
#                 value = issue.get(feat, '')
#                 if value and str(value) != 'None':
#                     readable = str(value).replace('_', ' ')
#                     text_parts.append(readable)
            
#             # Traditional categorical (low weight)
#             for feat in self.traditional_categorical:
#                 value = issue.get(feat, '')
#                 if value and str(value) != 'None':
#                     text_parts.append(str(value))
            
#             final_text = ' '.join(filter(None, text_parts))
#             texts.append(final_text if final_text else 'No description available')
        
#         # Generate embeddings
#         embeddings = self.embedding_model.encode(
#             texts,
#             batch_size=32,
#             show_progress_bar=True,
#             normalize_embeddings=True
#         )
        
#         return embeddings

#     def calculate_feature_similarity(self, issue1: Dict[str, Any], issue2: Dict[str, Any]) -> Dict[str, float]:
#         """Calculate similarity across different feature types"""
#         similarities = {}
        
#         # 1. Manual features similarity (exact matches)
#         manual_matches = 0
#         manual_total = 0
#         manual_details = []
        
#         for feat in self.manual_features:
#             val1 = issue1.get(feat)
#             val2 = issue2.get(feat)
#             if val1 is not None and val2 is not None and str(val1) != 'None' and str(val2) != 'None':
#                 manual_total += 1
#                 if val1 == val2:
#                     manual_matches += 1
#                     feat_name = feat.replace('feat_manual_', '').replace('_', ' ').title()
#                     manual_details.append(f"Same {feat_name}: {val1}")
        
#         similarities['manual_similarity'] = manual_matches / manual_total if manual_total > 0 else 0
#         similarities['manual_matches'] = manual_details
        
#         # 2. Clustering features similarity
#         clustering_matches = 0
#         clustering_total = 0
#         clustering_details = []
        
#         for feat in self.clustering_features:
#             val1 = issue1.get(feat)
#             val2 = issue2.get(feat)
#             if val1 is not None and val2 is not None and str(val1) != 'None' and str(val2) != 'None':
#                 clustering_total += 1
#                 if val1 == val2:
#                     clustering_matches += 1
#                     feat_name = feat.replace('feat_clustering_', '').replace('_', ' ').title()
#                     clustering_details.append(f"Same {feat_name}: {val1}")
        
#         similarities['clustering_similarity'] = clustering_matches / clustering_total if clustering_total > 0 else 0
#         similarities['clustering_matches'] = clustering_details
        
#         # 3. Traditional categorical similarity
#         traditional_matches = 0
#         traditional_total = 0
#         traditional_details = []
        
#         for feat in self.traditional_categorical:
#             val1 = issue1.get(feat)
#             val2 = issue2.get(feat)
#             if val1 is not None and val2 is not None and str(val1) != 'None' and str(val2) != 'None':
#                 traditional_total += 1
#                 if val1 == val2:
#                     traditional_matches += 1
#                     feat_name = feat.replace('feat_', '').replace('_', ' ').title()
#                     traditional_details.append(f"Same {feat_name}: {val1}")
        
#         similarities['traditional_similarity'] = traditional_matches / traditional_total if traditional_total > 0 else 0
#         similarities['traditional_matches'] = traditional_details
        
#         # 4. Numerical features similarity
#         numerical_similarities = []
#         numerical_details = []
        
#         for feat in self.numerical_features:
#             val1 = issue1.get(feat, 0)
#             val2 = issue2.get(feat, 0)
            
#             if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
#                 if feat == 'feat_project_cost':
#                     # Special handling for cost
#                     if val1 > 0 and val2 > 0:
#                         ratio = min(val1, val2) / max(val1, val2)
#                         if ratio > 0.7:  # Within 30%
#                             numerical_similarities.append(ratio)
#                             numerical_details.append(f"Similar project cost ({val1:.0f} vs {val2:.0f} Cr)")
#                 else:
#                     # Standard numerical similarity
#                     if max(abs(val1), abs(val2)) > 0:
#                         diff = abs(val1 - val2) / max(abs(val1), abs(val2), 1)
#                         similarity = max(0, 1 - diff)
#                         if similarity > 0.8:  # High similarity threshold
#                             numerical_similarities.append(similarity)
#                             if feat == 'feat_time_overrun_percent':
#                                 numerical_details.append(f"Similar delay levels ({val1:.0f}% vs {val2:.0f}%)")
#                             elif feat == 'feat_physical_progress':
#                                 numerical_details.append(f"Similar progress ({val1:.0f}% vs {val2:.0f}%)")
        
#         similarities['numerical_similarity'] = np.mean(numerical_similarities) if numerical_similarities else 0
#         similarities['numerical_matches'] = numerical_details
        
#         # 5. Combined feature similarity score
#         combined_score = (
#             similarities['manual_similarity'] * 0.4 +
#             similarities['clustering_similarity'] * 0.3 +
#             similarities['traditional_similarity'] * 0.2 +
#             similarities['numerical_similarity'] * 0.1
#         )
#         similarities['combined_feature_score'] = combined_score
        
#         return similarities

#     def generate_llm_similarity_analysis(self, query_issue: Dict[str, Any], similar_issue: Dict[str, Any], 
#                                       feature_similarities: Dict[str, Any]) -> str:
#         """Generate LLM-based similarity reasoning"""
#         if not self.use_llm:
#             return "LLM analysis disabled"
        
#         # Prepare context for LLM
#         query_context = self._prepare_issue_context(query_issue)
#         similar_context = self._prepare_issue_context(similar_issue)
        
#         prompt = f"""Analyze why these two Gati Shakti infrastructure issues are similar and provide reasoning:

# QUERY ISSUE:
# Title: {query_issue.get('issueTitle', 'N/A')}
# Description: {query_issue.get('issueDescription', 'N/A')[:300]}...
# Issue Type: {query_issue.get('feat_manual_issue_type', 'N/A')}
# Root Cause: {query_issue.get('feat_clustering_root_cause', 'N/A')}
# Resolution Strategy: {query_issue.get('feat_manual_resolution_strategy', 'N/A')}
# Sector: {query_issue.get('feat_sector', 'N/A')}
# Timeline Criticality: {query_issue.get('feat_manual_timeline_criticality', 'N/A')}

# SIMILAR ISSUE:
# Title: {similar_issue.get('issueTitle', 'N/A')}
# Description: {similar_issue.get('issueDescription', 'N/A')[:300]}...
# Issue Type: {similar_issue.get('feat_manual_issue_type', 'N/A')}
# Root Cause: {similar_issue.get('feat_clustering_root_cause', 'N/A')}
# Resolution Strategy: {similar_issue.get('feat_manual_resolution_strategy', 'N/A')}
# Sector: {similar_issue.get('feat_sector', 'N/A')}
# Timeline Criticality: {similar_issue.get('feat_manual_timeline_criticality', 'N/A')}

# FEATURE MATCHES:
# Manual Feature Matches: {len(feature_similarities.get('manual_matches', []))}
# Clustering Pattern Matches: {len(feature_similarities.get('clustering_matches', []))}
# Traditional Category Matches: {len(feature_similarities.get('traditional_matches', []))}

# Analyze the similarity and provide:
# 1. Primary similarity reasons (top 3)
# 2. Common patterns between the issues
# 3. Potential solution approaches that could work for both
# 4. Risk factors common to both issues

# Keep response concise (under 200 words) and focus on actionable insights."""

#         try:
#             response = self._call_llm_api(prompt)
#             return response.strip()
#         except Exception as e:
#             logger.error(f"LLM analysis failed: {e}")
#             return f"LLM analysis failed. Feature similarity score: {feature_similarities.get('combined_feature_score', 0):.3f}"

#     def _prepare_issue_context(self, issue: Dict[str, Any]) -> str:
#         """Prepare concise issue context for LLM"""
#         context_parts = [
#             issue.get('issueTitle', ''),
#             issue.get('feat_manual_issue_type', ''),
#             issue.get('feat_sector', ''),
#             issue.get('feat_clustering_root_cause', '')
#         ]
#         return ' | '.join(filter(None, context_parts))

#     def find_similar_issues(self, query_issue: Dict[str, Any], all_issues: List[Dict[str, Any]], 
#                           all_embeddings: np.ndarray, query_idx: int, k: int = 4) -> List[Dict[str, Any]]:
#         """Find top k similar issues for a query issue"""
        
#         # Get query embedding
#         query_embedding = all_embeddings[query_idx].reshape(1, -1)
        
#         # Calculate text similarity with all other issues
#         text_similarities = cosine_similarity(query_embedding, all_embeddings).flatten()
        
#         # Get candidate indices (exclude self)
#         candidate_indices = [i for i in range(len(all_issues)) if i != query_idx]
        
#         # Calculate comprehensive similarity scores
#         similarity_results = []
        
#         for idx in candidate_indices:
#             candidate_issue = all_issues[idx]
            
#             # Text similarity
#             text_sim = text_similarities[idx]
            
#             # Feature similarities
#             feature_sims = self.calculate_feature_similarity(query_issue, candidate_issue)
            
#             # Combined similarity score
#             combined_score = (
#                 text_sim * self.similarity_weights['text_similarity'] +
#                 feature_sims['manual_similarity'] * self.similarity_weights['manual_features'] +
#                 feature_sims['clustering_similarity'] * self.similarity_weights['clustering_features'] +
#                 feature_sims['numerical_similarity'] * self.similarity_weights['numerical_features']
#             )
            
#             # Compile all match details
#             all_matches = []
#             all_matches.extend(feature_sims.get('manual_matches', []))
#             all_matches.extend(feature_sims.get('clustering_matches', []))
#             all_matches.extend(feature_sims.get('traditional_matches', []))
#             all_matches.extend(feature_sims.get('numerical_matches', []))
            
#             similarity_results.append({
#                 'issue_idx': idx,
#                 'issue': candidate_issue,
#                 'text_similarity': float(text_sim),
#                 'feature_similarity': feature_sims['combined_feature_score'],
#                 'combined_similarity': float(combined_score),
#                 'similarity_details': feature_sims,
#                 'match_reasons': all_matches
#             })
        
#         # Sort by combined similarity
#         similarity_results.sort(key=lambda x: x['combined_similarity'], reverse=True)
        
#         # Get top k results
#         top_similar = similarity_results[:k]
        
#         # Generate LLM analysis for top results
#         for result in top_similar:
#             result['llm_analysis'] = self.generate_llm_similarity_analysis(
#                 query_issue, result['issue'], result['similarity_details']
#             )
        
#         return top_similar

#     def process_all_issues(self, input_file: str, output_file: str) -> None:
#         """Process all issues to find similar ones"""
#         logger.info(f"Loading issues from {input_file}")
        
#         # Load data
#         with open(input_file, 'r', encoding='utf-8') as f:
#             issues_data = json.load(f)
        
#         logger.info(f"Processing {len(issues_data)} issues")
        
#         # Create embeddings for all issues
#         all_embeddings = self.create_issue_embeddings(issues_data)
        
#         # Process each issue
#         results = []
        
#         for i, issue in enumerate(issues_data):
#             if i % 10 == 0:
#                 logger.info(f"Processing issue {i+1}/{len(issues_data)}")
            
#             # Create issue key
#             issue_key = issue.get('issue_key', f"issue_{i}")
            
#             # Find similar issues
#             similar_issues = self.find_similar_issues(
#                 query_issue=issue,
#                 all_issues=issues_data,
#                 all_embeddings=all_embeddings,
#                 query_idx=i,
#                 k=4
#             )
            
#             # Prepare similar issues data
#             similar_issues_data = []
#             for sim in similar_issues:
#                 similar_data = {
#                     'issue_key': sim['issue'].get('issue_key', f"issue_{sim['issue_idx']}"),
#                     'title': sim['issue'].get('issueTitle', 'No title'),
#                     'sector': sim['issue'].get('feat_sector', 'Unknown'),
#                     'issue_category': sim['issue'].get('feat_issue_category', 'Unknown'),
#                     'text_similarity_score': sim['text_similarity'],
#                     'feature_similarity_score': sim['feature_similarity'],
#                     'combined_similarity_score': sim['combined_similarity'],
#                     'similarity_reasons': sim['match_reasons'][:5],  # Top 5 reasons
#                     'llm_analysis': sim['llm_analysis'],
#                     'manual_issue_type': sim['issue'].get('feat_manual_issue_type', 'Unknown'),
#                     'resolution_strategy': sim['issue'].get('feat_manual_resolution_strategy', 'Unknown'),
#                     'timeline_criticality': sim['issue'].get('feat_manual_timeline_criticality', 'Unknown'),
#                     'root_cause': sim['issue'].get('feat_clustering_root_cause', 'Unknown')
#                 }
#                 similar_issues_data.append(similar_data)
            
#             # Create result entry
#             result_entry = {
#                 'query_issue': {
#                     'issue_key': issue_key,
#                     'title': issue.get('issueTitle', 'No title'),
#                     'description': issue.get('issueDescription', 'No description')[:200] + '...',
#                     'sector': issue.get('feat_sector', 'Unknown'),
#                     'issue_category': issue.get('feat_issue_category', 'Unknown'),
#                     'manual_issue_type': issue.get('feat_manual_issue_type', 'Unknown'),
#                     'resolution_strategy': issue.get('feat_manual_resolution_strategy', 'Unknown'),
#                     'timeline_criticality': issue.get('feat_manual_timeline_criticality', 'Unknown'),
#                     'root_cause': issue.get('feat_clustering_root_cause', 'Unknown'),
#                     'project_cost': issue.get('feat_project_cost', 0),
#                     'time_overrun_percent': issue.get('feat_time_overrun_percent', 0),
#                     'urgency_score': issue.get('feat_urgency_score', 0)
#                 },
#                 'similar_issues': similar_issues_data,
#                 'analysis_metadata': {
#                     'processed_at': datetime.now().isoformat(),
#                     'similarity_weights': self.similarity_weights,
#                     'total_similar_found': len(similar_issues_data)
#                 }
#             }
            
#             results.append(result_entry)

#             if i%10 == 0:
#                 with open(output_file, 'w', encoding='utf-8') as f:
#                     json.dump(results, f, indent=2, ensure_ascii=False, default=str)
        
#         # Save results
#         logger.info(f"Saving results to {output_file}")
#         with open(output_file, 'w', encoding='utf-8') as f:
#             json.dump(results, f, indent=2, ensure_ascii=False, default=str)
        
#         logger.info("Similar issues analysis completed successfully!")
        
#         # Print summary statistics
#         self._print_summary_statistics(results)

#     def _print_summary_statistics(self, results: List[Dict[str, Any]]) -> None:
#         """Print summary statistics of the analysis"""
#         print("\n" + "="*60)
#         print("SIMILAR ISSUES ANALYSIS SUMMARY")
#         print("="*60)
        
#         total_issues = len(results)
        
#         # Similarity score statistics
#         all_similarities = []
#         for result in results:
#             for similar in result['similar_issues']:
#                 all_similarities.append(similar['combined_similarity_score'])
        
#         if all_similarities:
#             print(f"Total Issues Processed: {total_issues}")
#             print(f"Total Similarity Pairs: {len(all_similarities)}")
#             print(f"Average Similarity Score: {np.mean(all_similarities):.3f}")
#             print(f"Median Similarity Score: {np.median(all_similarities):.3f}")
#             print(f"Max Similarity Score: {np.max(all_similarities):.3f}")
#             print(f"High Similarity Pairs (>0.7): {len([s for s in all_similarities if s > 0.7])}")
        
#         # Feature match statistics
#         manual_matches = 0
#         clustering_matches = 0
#         traditional_matches = 0
        
#         for result in results:
#             for similar in result['similar_issues']:
#                 manual_matches += len([r for r in similar['similarity_reasons'] if 'Issue Type' in r or 'Complexity' in r])
#                 clustering_matches += len([r for r in similar['similarity_reasons'] if 'Pattern' in r or 'Cause' in r])
#                 traditional_matches += len([r for r in similar['similarity_reasons'] if 'Category' in r or 'Sector' in r])
        
#         print(f"\nFeature Match Statistics:")
#         print(f"Manual Feature Matches: {manual_matches}")
#         print(f"Clustering Feature Matches: {clustering_matches}")
#         print(f"Traditional Feature Matches: {traditional_matches}")
        
#         # Example high similarity pair
#         high_sim_pairs = [(result, similar) for result in results 
#                          for similar in result['similar_issues'] 
#                          if similar['combined_similarity_score'] > 0.6]
        
#         if high_sim_pairs:
#             print(f"\nExample High Similarity Pair:")
#             example_result, example_similar = high_sim_pairs[0]
#             print(f"Query: {example_result['query_issue']['title'][:60]}...")
#             print(f"Similar: {example_similar['title'][:60]}...")
#             print(f"Similarity Score: {example_similar['combined_similarity_score']:.3f}")
#             print(f"Reasons: {', '.join(example_similar['similarity_reasons'][:3])}")

#     def _call_llm_api(self, prompt: str) -> str:
#         """Call LLM API for analysis"""
#         headers = {
#             "Authorization": f"Bearer {self.llm_token}",
#             "Cookie": "SERVERID=api-manager",
#             "Content-Type": "application/json"
#         }

#         payload = {
#             "model": "meta/llama-3.2-11b-vision-instruct",
#             "messages": [{"role": "user", "content": prompt}],
#             # "max_tokens": 300
#         }
        
#         try:
#             response = requests.post(self.llm_url, headers=headers, json=payload, verify=False, timeout=30)
#             response.raise_for_status()
#             result = response.json()
#             return result["choices"][0]["message"]["content"].strip()
#         except Exception as e:
#             logger.error(f"LLM API error: {e}")
#             return "LLM analysis not available"


# def main():
#     """Main function to run the similar issues analysis"""
#     print("="*70)
#     print("GATI SHAKTI SIMILAR ISSUES RAG PIPELINE")
#     print("="*70)
    
#     # Initialize the RAG pipeline
#     rag_pipeline = GatiShaktiSimilarIssuesRAG(use_llm=True)
    
#     # Define file paths
#     input_file = '../data/prod/gati_shakti_feature_engineering.json'
#     output_file = '../data/prod/gati_shakti_similar_issues_analysis2.json'
    
#     print(f"Input: {input_file}")
#     print(f"Output: {output_file}")
#     print("Starting analysis...")
    
#     try:
#         # Process all issues
#         rag_pipeline.process_all_issues(input_file, output_file)
        
#         print(f"\n Analysis completed successfully!")
#         print(f" Results saved to: {output_file}")
        
#     except FileNotFoundError:
#         print(f" Error: Input file not found at {input_file}")
#         print("Please ensure the feature engineering pipeline has been run first.")
#     except Exception as e:
#         print(f" Error during analysis: {e}")


# def load_and_analyze_results(results_file: str):
#     """Load and analyze the similar issues results"""
#     try:
#         with open(results_file, 'r', encoding='utf-8') as f:
#             results = json.load(f)
        
#         print(f"Loaded {len(results)} issue analyses")
        
#         # Show example results
#         for i, result in enumerate(results[:3]):
#             print(f"\n--- Example {i+1} ---")
#             print(f"Query Issue: {result['query_issue']['title']}")
#             print(f"Sector: {result['query_issue']['sector']}")
#             print(f"Issue Type: {result['query_issue']['manual_issue_type']}")
            
#             print(f"\nTop Similar Issues:")
#             for j, similar in enumerate(result['similar_issues'][:2], 1):
#                 print(f"  {j}. {similar['title'][:50]}...")
#                 print(f"     Similarity: {similar['combined_similarity_score']:.3f}")
#                 print(f"     Reasons: {', '.join(similar['similarity_reasons'][:2])}")
        
#     except FileNotFoundError:
#         print(f"Results file not found at {results_file}")
#     except Exception as e:
#         print(f"Error loading results: {e}")


# if __name__ == "__main__":
#     main()
    
#     # Uncomment to analyze results after running main()
#     # load_and_analyze_results('../data/prod/gati_shakti_similar_issues_analysis.json')




import json
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple
import logging
from datetime import datetime
import requests
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GatiShaktiSimilarIssuesRAG:
    """
    Enhanced RAG pipeline for finding similar issues with comprehensive reasoning
    """
    
    def __init__(self, use_llm: bool = True):
        self.use_llm = use_llm
        
        # LLM Configuration
        self.llm_url = "https://apis.airawat.cdac.in/negd/v1/chat/completions"
        self.llm_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJ5YUtYUGtLZ1lKSUNqRTg3dHUwVnBUS3R5UUhxaUk1cyJ9.wJbzY_YRXGBug9sO1EYSwiCkRrI1bKnOL3uxSITxV94"
        
        # Initialize embedding model
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        
        # Key features for similarity analysis
        self.manual_features = [
            'feat_manual_issue_type',
            'feat_manual_stakeholder_complexity',
            'feat_manual_resolution_strategy', 
            'feat_manual_timeline_criticality',
            'feat_manual_geographic_scope',
            'feat_manual_bureaucratic_complexity',
            'feat_manual_environmental_impact',
            'feat_manual_financial_complexity',
            'feat_manual_seasonal_dependency',
            'feat_manual_public_sensitivity',
            'feat_manual_legal_risk',
            'feat_manual_tech_dependency',
            'feat_manual_infrastructure_type',
            'feat_manual_displacement_scale'
        ]
        
        self.clustering_features = [
            'feat_clustering_issue_pattern',
            'feat_clustering_root_cause',
            'feat_clustering_solution_approach',
            'feat_clustering_recurrence_pattern',
            'feat_clustering_stakeholder_pattern',
            'feat_clustering_communication_urgency',
            'feat_clustering_evolution_stage'
        ]

        self.time_features = [
            'feat_delay_time',
            'feat_diff_resolved_start',
            'feat_diff_tentative_start', 
            'feat_diff_hold_start',
            'feat_diff_today_hold',
            'feat_diff_today_pending_pmg',
            'feat_diff_today_pending_rm_rs',
            'feat_diff_today_tentative'
        ]
        
        self.traditional_categorical = [
            'feat_issue_category',
            'feat_sector',
            'feat_region',
            'feat_project_stage',
            'feat_responsible_authority'
        ]
        
        self.numerical_features = [
            'feat_project_cost',
            'feat_time_overrun_percent',
            'feat_delay_risk_score',
            'feat_urgency_score',
            'feat_stakeholder_complexity',
            'feat_physical_progress',
            'feat_financial_progress',
            'feat_delay_time',
            'feat_diff_resolved_start',
            'feat_diff_tentative_start', 
            'feat_diff_hold_start',
            'feat_diff_today_hold',
            'feat_diff_today_pending_pmg',
            'feat_diff_today_pending_rm_rs',
            'feat_diff_today_tentative'
        ]
        
        # Weights for different similarity components
        self.similarity_weights = {
            'text_similarity': 0.3,
            'manual_features': 0.25,
            'clustering_features': 0.2,
            'time_features': 0.15,
            'numerical_features': 0.1
        }

    def create_issue_embeddings(self, issues_data: List[Dict[str, Any]]) -> np.ndarray:
        """Create comprehensive text embeddings for all issues"""
        logger.info("Creating text embeddings for all issues...")
        
        texts = []
        for issue in issues_data:
            # Combine multiple text fields with weights
            text_parts = []
            
            # Primary text fields (high weight)
            combined_text = issue.get('combined_text', '')
            if combined_text:
                text_parts.extend([combined_text] * 3)
            
            title = issue.get('issueTitle', '')
            if title:
                text_parts.extend([title] * 2)
                
            description = issue.get('issueDescription', '')
            if description:
                text_parts.extend([description] * 2)
            
            # Feature-based text (medium weight)
            for feat in self.manual_features + self.clustering_features:
                value = issue.get(feat, '')
                if value and str(value) != 'None':
                    readable = str(value).replace('_', ' ')
                    text_parts.append(readable)
            
            # Traditional categorical (low weight)
            for feat in self.traditional_categorical:
                value = issue.get(feat, '')
                if value and str(value) != 'None':
                    text_parts.append(str(value))
            
            final_text = ' '.join(filter(None, text_parts))
            texts.append(final_text if final_text else 'No description available')
        
        # Generate embeddings
        embeddings = self.embedding_model.encode(
            texts,
            batch_size=32,
            show_progress_bar=True,
            normalize_embeddings=True
        )
        
        return embeddings

    def calculate_feature_similarity(self, issue1: Dict[str, Any], issue2: Dict[str, Any]) -> Dict[str, float]:
        """Calculate similarity across different feature types"""
        similarities = {}
        
        # 1. Manual features similarity (exact matches)
        manual_matches = 0
        manual_total = 0
        manual_details = []
        
        for feat in self.manual_features:
            val1 = issue1.get(feat)
            val2 = issue2.get(feat)
            if val1 is not None and val2 is not None and str(val1) != 'None' and str(val2) != 'None':
                manual_total += 1
                if val1 == val2:
                    manual_matches += 1
                    feat_name = feat.replace('feat_manual_', '').replace('_', ' ').title()
                    manual_details.append(f"Same {feat_name}: {val1}")
        
        similarities['manual_similarity'] = manual_matches / manual_total if manual_total > 0 else 0
        similarities['manual_matches'] = manual_details
        
        # 2. Clustering features similarity
        clustering_matches = 0
        clustering_total = 0
        clustering_details = []
        
        for feat in self.clustering_features:
            val1 = issue1.get(feat)
            val2 = issue2.get(feat)
            if val1 is not None and val2 is not None and str(val1) != 'None' and str(val2) != 'None':
                clustering_total += 1
                if val1 == val2:
                    clustering_matches += 1
                    feat_name = feat.replace('feat_clustering_', '').replace('_', ' ').title()
                    clustering_details.append(f"Same {feat_name}: {val1}")
        
        similarities['clustering_similarity'] = clustering_matches / clustering_total if clustering_total > 0 else 0
        similarities['clustering_matches'] = clustering_details
        
        # 3. Traditional categorical similarity
        traditional_matches = 0
        traditional_total = 0
        traditional_details = []
        
        for feat in self.traditional_categorical:
            val1 = issue1.get(feat)
            val2 = issue2.get(feat)
            if val1 is not None and val2 is not None and str(val1) != 'None' and str(val2) != 'None':
                traditional_total += 1
                if val1 == val2:
                    traditional_matches += 1
                    feat_name = feat.replace('feat_', '').replace('_', ' ').title()
                    traditional_details.append(f"Same {feat_name}: {val1}")
        
        similarities['traditional_similarity'] = traditional_matches / traditional_total if traditional_total > 0 else 0
        similarities['traditional_matches'] = traditional_details
        
        # 4. Numerical features similarity
        numerical_similarities = []
        numerical_details = []
        
        for feat in self.numerical_features:
            val1 = issue1.get(feat, 0)
            val2 = issue2.get(feat, 0)
            
            if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
                if feat == 'feat_project_cost':
                    # Special handling for cost
                    if val1 > 0 and val2 > 0:
                        ratio = min(val1, val2) / max(val1, val2)
                        if ratio > 0.7:  # Within 30%
                            numerical_similarities.append(ratio)
                            numerical_details.append(f"Similar project cost ({val1:.0f} vs {val2:.0f} Cr)")
                else:
                    # Standard numerical similarity
                    if max(abs(val1), abs(val2)) > 0:
                        diff = abs(val1 - val2) / max(abs(val1), abs(val2), 1)
                        similarity = max(0, 1 - diff)
                        if similarity > 0.8:  # High similarity threshold
                            numerical_similarities.append(similarity)
                            if feat == 'feat_time_overrun_percent':
                                numerical_details.append(f"Similar delay levels ({val1:.0f}% vs {val2:.0f}%)")
                            elif feat == 'feat_physical_progress':
                                numerical_details.append(f"Similar progress ({val1:.0f}% vs {val2:.0f}%)")
        
        similarities['numerical_similarity'] = np.mean(numerical_similarities) if numerical_similarities else 0
        similarities['numerical_matches'] = numerical_details


        # 5. Time features similarity (add this new section)
        time_similarities = []
        time_details = []

        for feat in self.time_features:
            val1 = issue1.get(feat, 0)
            val2 = issue2.get(feat, 0)
            
            if isinstance(val1, (int, float)) and isinstance(val2, (int, float)) and val1 is not None and val2 is not None:
                if val1 > 0 and val2 > 0:  # Both have valid time values
                    if feat in ['feat_diff_resolved_start', 'feat_delay_time']:
                        # For resolution times, closer values are more similar
                        diff = abs(val1 - val2)
                        max_val = max(val1, val2)
                        if max_val > 0:
                            similarity = max(0, 1 - (diff / max_val))
                            if similarity > 0.7:
                                time_similarities.append(similarity)
                                time_details.append(f"Similar resolution time ({val1:.0f} vs {val2:.0f} days)")
                    
                    elif 'today' in feat:
                        # For current status times, exact matches or close values
                        if abs(val1 - val2) <= 7:  # Within a week
                            time_similarities.append(0.9)
                            time_details.append(f"Similar current status timing")

        similarities['time_similarity'] = np.mean(time_similarities) if time_similarities else 0
        similarities['time_matches'] = time_details

        # 6. Update combined feature similarity score
        combined_score = (
            similarities['manual_similarity'] * 0.35 +
            similarities['clustering_similarity'] * 0.25 +
            similarities['time_similarity'] * 0.2 +      # New component
            similarities['traditional_similarity'] * 0.15 +
            similarities['numerical_similarity'] * 0.05
        )
        
        # # 5. Combined feature similarity score
        # combined_score = (
        #     similarities['manual_similarity'] * 0.4 +
        #     similarities['clustering_similarity'] * 0.3 +
        #     similarities['traditional_similarity'] * 0.2 +
        #     similarities['numerical_similarity'] * 0.1
        # )
        similarities['combined_feature_score'] = combined_score
        
        return similarities

    def generate_llm_similarity_analysis(self, query_issue: Dict[str, Any], similar_issue: Dict[str, Any], 
                                      feature_similarities: Dict[str, Any]) -> str:
        """Generate LLM-based similarity reasoning"""
        if not self.use_llm:
            return "LLM analysis disabled"
        
        # Prepare context for LLM
        query_context = self._prepare_issue_context(query_issue)
        similar_context = self._prepare_issue_context(similar_issue)
        
        prompt = f"""Analyze why these two Gati Shakti infrastructure issues are similar and provide reasoning:

QUERY ISSUE:
Title: {query_issue.get('issueTitle', 'N/A')}
Description: {query_issue.get('issueDescription', 'N/A')[:300]}...
Issue Type: {query_issue.get('feat_manual_issue_type', 'N/A')}
Root Cause: {query_issue.get('feat_clustering_root_cause', 'N/A')}
Resolution Strategy: {query_issue.get('feat_manual_resolution_strategy', 'N/A')}
Sector: {query_issue.get('feat_sector', 'N/A')}
Timeline Criticality: {query_issue.get('feat_manual_timeline_criticality', 'N/A')}

SIMILAR ISSUE:
Title: {similar_issue.get('issueTitle', 'N/A')}
Description: {similar_issue.get('issueDescription', 'N/A')[:300]}...
Issue Type: {similar_issue.get('feat_manual_issue_type', 'N/A')}
Root Cause: {similar_issue.get('feat_clustering_root_cause', 'N/A')}
Resolution Strategy: {similar_issue.get('feat_manual_resolution_strategy', 'N/A')}
Sector: {similar_issue.get('feat_sector', 'N/A')}
Timeline Criticality: {similar_issue.get('feat_manual_timeline_criticality', 'N/A')}

FEATURE MATCHES:
Manual Feature Matches: {len(feature_similarities.get('manual_matches', []))}
Clustering Pattern Matches: {len(feature_similarities.get('clustering_matches', []))}
Traditional Category Matches: {len(feature_similarities.get('traditional_matches', []))}

Analyze the similarity and provide:
1. Primary similarity reasons (top 3)
2. Common patterns between the issues
3. Potential solution approaches that could work for both
4. Risk factors common to both issues

Keep response concise (under 200 words) and focus on actionable insights."""

        try:
            response = self._call_llm_api(prompt)
            return response.strip()
        except Exception as e:
            logger.error(f"LLM analysis failed: {e}")
            return f"LLM analysis failed. Feature similarity score: {feature_similarities.get('combined_feature_score', 0):.3f}"

    def _prepare_issue_context(self, issue: Dict[str, Any]) -> str:
        """Prepare concise issue context for LLM"""
        context_parts = [
            issue.get('issueTitle', ''),
            issue.get('feat_manual_issue_type', ''),
            issue.get('feat_sector', ''),
            issue.get('feat_clustering_root_cause', '')
        ]
        return ' | '.join(filter(None, context_parts))

    def find_similar_issues(self, query_issue: Dict[str, Any], all_issues: List[Dict[str, Any]], 
                          all_embeddings: np.ndarray, query_idx: int, k: int = 4) -> List[Dict[str, Any]]:
        """Find top k similar issues for a query issue"""
        
        # Get query embedding
        query_embedding = all_embeddings[query_idx].reshape(1, -1)
        
        # Calculate text similarity with all other issues
        text_similarities = cosine_similarity(query_embedding, all_embeddings).flatten()
        
        # Get candidate indices (exclude self)
        # candidate_indices = [i for i in range(len(all_issues)) if i != query_idx]
        # Get candidate indices (exclude self and same project)
        query_project_id = query_issue.get('projectId', '')
        candidate_indices = [
            i for i in range(len(all_issues)) 
            if i != query_idx and all_issues[i].get('projectId', '') != query_project_id
        ]
        
        # Calculate comprehensive similarity scores
        similarity_results = []
        
        for idx in candidate_indices:
            candidate_issue = all_issues[idx]
            
            # Text similarity
            text_sim = text_similarities[idx]
            
            # Feature similarities
            feature_sims = self.calculate_feature_similarity(query_issue, candidate_issue)
            
            # # Combined similarity score
            # combined_score = (
            #     text_sim * self.similarity_weights['text_similarity'] +
            #     feature_sims['manual_similarity'] * self.similarity_weights['manual_features'] +
            #     feature_sims['clustering_similarity'] * self.similarity_weights['clustering_features'] +
            #     feature_sims['numerical_similarity'] * self.similarity_weights['numerical_features']
            # )
            
            # # Compile all match details
            # all_matches = []
            # all_matches.extend(feature_sims.get('manual_matches', []))
            # all_matches.extend(feature_sims.get('clustering_matches', []))
            # all_matches.extend(feature_sims.get('traditional_matches', []))
            # all_matches.extend(feature_sims.get('numerical_matches', []))

                        # Combined similarity score
            combined_score = (
                text_sim * self.similarity_weights['text_similarity'] +
                feature_sims['manual_similarity'] * self.similarity_weights['manual_features'] +
                feature_sims['clustering_similarity'] * self.similarity_weights['clustering_features'] +
                feature_sims['time_similarity'] * self.similarity_weights['time_features'] +  # Add this line
                feature_sims['numerical_similarity'] * self.similarity_weights['numerical_features']
            )

            # Update all_matches to include time matches
            all_matches = []
            all_matches.extend(feature_sims.get('manual_matches', []))
            all_matches.extend(feature_sims.get('clustering_matches', []))
            all_matches.extend(feature_sims.get('time_matches', []))        # Add this line
            all_matches.extend(feature_sims.get('traditional_matches', []))
            all_matches.extend(feature_sims.get('numerical_matches', []))
            
            similarity_results.append({
                'issue_idx': idx,
                'issue': candidate_issue,
                'text_similarity': float(text_sim),
                'feature_similarity': feature_sims['combined_feature_score'],
                'combined_similarity': float(combined_score),
                'similarity_details': feature_sims,
                'match_reasons': all_matches
            })
        
        # Sort by combined similarity
        similarity_results.sort(key=lambda x: x['combined_similarity'], reverse=True)
        
        # Get top k results
        top_similar = similarity_results[:k]
        
        # Generate LLM analysis for top results
        for result in top_similar:
            result['llm_analysis'] = self.generate_llm_similarity_analysis(
                query_issue, result['issue'], result['similarity_details']
            )
        
        return top_similar

    def process_all_issues(self, input_file: str, output_file: str) -> None:
        """Process all issues to find similar ones"""
        logger.info(f"Loading issues from {input_file}")
        
        # Load data
        with open(input_file, 'r', encoding='utf-8') as f:
            issues_data = json.load(f)
        
        logger.info(f"Processing {len(issues_data)} issues")
        
        # Create embeddings for all issues
        all_embeddings = self.create_issue_embeddings(issues_data)
        
        # Process each issue
        results = []
        
        for i, issue in enumerate(issues_data):
            if i % 10 == 0:
                logger.info(f"Processing issue {i+1}/{len(issues_data)}")
            
            # Create issue key
            issue_key = issue.get('issue_key', f"issue_{i}")
            
            # Find similar issues
            similar_issues = self.find_similar_issues(
                query_issue=issue,
                all_issues=issues_data,
                all_embeddings=all_embeddings,
                query_idx=i,
                k=4
            )
            
            # Prepare similar issues data
            similar_issues_data = []
            for sim in similar_issues:
                similar_data = {
                    'issue_key': sim['issue'].get('issue_key', f"issue_{sim['issue_idx']}"),
                    'title': sim['issue'].get('issueTitle', 'No title'),
                    'sector': sim['issue'].get('feat_sector', 'Unknown'),
                    'issue_category': sim['issue'].get('feat_issue_category', 'Unknown'),
                    'text_similarity_score': sim['text_similarity'],
                    'feature_similarity_score': sim['feature_similarity'],
                    'combined_similarity_score': sim['combined_similarity'],
                    'similarity_reasons': sim['match_reasons'][:5],  # Top 5 reasons
                    'llm_analysis': sim['llm_analysis'],
                    'manual_issue_type': sim['issue'].get('feat_manual_issue_type', 'Unknown'),
                    'resolution_strategy': sim['issue'].get('feat_manual_resolution_strategy', 'Unknown'),
                    'timeline_criticality': sim['issue'].get('feat_manual_timeline_criticality', 'Unknown'),
                    'root_cause': sim['issue'].get('feat_clustering_root_cause', 'Unknown'),
                    'delay_time': sim['issue'].get('feat_delay_time'),
                    'resolution_time_days': sim['issue'].get('feat_diff_resolved_start'),
                    'current_status_days': sim['issue'].get('feat_diff_today_pending_pmg')
                }
                similar_issues_data.append(similar_data)
            
            # Create result entry
            result_entry = {
                'query_issue': {
                    'issue_key': issue_key,
                    'title': issue.get('issueTitle', 'No title'),
                    'description': issue.get('issueDescription', 'No description')[:200] + '...',
                    'sector': issue.get('feat_sector', 'Unknown'),
                    'issue_category': issue.get('feat_issue_category', 'Unknown'),
                    'manual_issue_type': issue.get('feat_manual_issue_type', 'Unknown'),
                    'resolution_strategy': issue.get('feat_manual_resolution_strategy', 'Unknown'),
                    'timeline_criticality': issue.get('feat_manual_timeline_criticality', 'Unknown'),
                    'root_cause': issue.get('feat_clustering_root_cause', 'Unknown'),
                    'project_cost': issue.get('feat_project_cost', 0),
                    'time_overrun_percent': issue.get('feat_time_overrun_percent', 0),
                    'urgency_score': issue.get('feat_urgency_score', 0),
                    'delay_time': issue.get('feat_delay_time'),
                    'resolution_time_days': issue.get('feat_diff_resolved_start'),
                    'current_status_days': issue.get('feat_diff_today_pending_pmg')
                },
                'similar_issues': similar_issues_data,
                'analysis_metadata': {
                    'processed_at': datetime.now().isoformat(),
                    'similarity_weights': self.similarity_weights,
                    'total_similar_found': len(similar_issues_data)
                }
            }
            
            results.append(result_entry)

            if i%10 == 0:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(results, f, indent=2, ensure_ascii=False, default=str)
        
        # Save results
        logger.info(f"Saving results to {output_file}")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info("Similar issues analysis completed successfully!")
        
        # Print summary statistics
        self._print_summary_statistics(results)

    def _print_summary_statistics(self, results: List[Dict[str, Any]]) -> None:
        """Print summary statistics of the analysis"""
        print("\n" + "="*60)
        print("SIMILAR ISSUES ANALYSIS SUMMARY")
        print("="*60)
        
        total_issues = len(results)
        
        # Similarity score statistics
        all_similarities = []
        for result in results:
            for similar in result['similar_issues']:
                all_similarities.append(similar['combined_similarity_score'])
        
        if all_similarities:
            print(f"Total Issues Processed: {total_issues}")
            print(f"Total Similarity Pairs: {len(all_similarities)}")
            print(f"Average Similarity Score: {np.mean(all_similarities):.3f}")
            print(f"Median Similarity Score: {np.median(all_similarities):.3f}")
            print(f"Max Similarity Score: {np.max(all_similarities):.3f}")
            print(f"High Similarity Pairs (>0.7): {len([s for s in all_similarities if s > 0.7])}")
        
        # Feature match statistics
        manual_matches = 0
        clustering_matches = 0
        traditional_matches = 0
        
        for result in results:
            for similar in result['similar_issues']:
                manual_matches += len([r for r in similar['similarity_reasons'] if 'Issue Type' in r or 'Complexity' in r])
                clustering_matches += len([r for r in similar['similarity_reasons'] if 'Pattern' in r or 'Cause' in r])
                traditional_matches += len([r for r in similar['similarity_reasons'] if 'Category' in r or 'Sector' in r])
        
        print(f"\nFeature Match Statistics:")
        print(f"Manual Feature Matches: {manual_matches}")
        print(f"Clustering Feature Matches: {clustering_matches}")
        print(f"Traditional Feature Matches: {traditional_matches}")
        
        # Example high similarity pair
        high_sim_pairs = [(result, similar) for result in results 
                         for similar in result['similar_issues'] 
                         if similar['combined_similarity_score'] > 0.6]
        
        if high_sim_pairs:
            print(f"\nExample High Similarity Pair:")
            example_result, example_similar = high_sim_pairs[0]
            print(f"Query: {example_result['query_issue']['title'][:60]}...")
            print(f"Similar: {example_similar['title'][:60]}...")
            print(f"Similarity Score: {example_similar['combined_similarity_score']:.3f}")
            print(f"Reasons: {', '.join(example_similar['similarity_reasons'][:3])}")

    def _call_llm_api(self, prompt: str) -> str:
        """Call LLM API for analysis"""
        headers = {
            "Authorization": f"Bearer {self.llm_token}",
            "Cookie": "SERVERID=api-manager",
            "Content-Type": "application/json"
        }

        payload = {
            "model": "meta/llama-3.2-11b-vision-instruct",
            "messages": [{"role": "user", "content": prompt}],
            # "max_tokens": 300
        }
        
        try:
            response = requests.post(self.llm_url, headers=headers, json=payload, verify=False, timeout=30)
            response.raise_for_status()
            result = response.json()
            return result["choices"][0]["message"]["content"].strip()
        except Exception as e:
            logger.error(f"LLM API error: {e}")
            return "LLM analysis not available"


def main():
    """Main function to run the similar issues analysis"""
    print("="*70)
    print("GATI SHAKTI SIMILAR ISSUES RAG PIPELINE")
    print("="*70)
    
    # Initialize the RAG pipeline
    rag_pipeline = GatiShaktiSimilarIssuesRAG(use_llm=True)
    
    # Define file paths
    input_file = '../data/prod/gati_shakti_feature_engineering_with_time_features.json'
    output_file = '../data/prod/gati_shakti_similar_issues_analysis_with_time_features2.json'
    
    print(f"Input: {input_file}")
    print(f"Output: {output_file}")
    print("Starting analysis...")
    
    try:
        # Process all issues
        rag_pipeline.process_all_issues(input_file, output_file)
        
        print(f"\n Analysis completed successfully!")
        print(f" Results saved to: {output_file}")
        
    except FileNotFoundError:
        print(f" Error: Input file not found at {input_file}")
        print("Please ensure the feature engineering pipeline has been run first.")
    except Exception as e:
        print(f" Error during analysis: {e}")


def load_and_analyze_results(results_file: str):
    """Load and analyze the similar issues results"""
    try:
        with open(results_file, 'r', encoding='utf-8') as f:
            results = json.load(f)
        
        print(f"Loaded {len(results)} issue analyses")
        
        # Show example results
        for i, result in enumerate(results[:3]):
            print(f"\n--- Example {i+1} ---")
            print(f"Query Issue: {result['query_issue']['title']}")
            print(f"Sector: {result['query_issue']['sector']}")
            print(f"Issue Type: {result['query_issue']['manual_issue_type']}")
            
            print(f"\nTop Similar Issues:")
            for j, similar in enumerate(result['similar_issues'][:2], 1):
                print(f"  {j}. {similar['title'][:50]}...")
                print(f"     Similarity: {similar['combined_similarity_score']:.3f}")
                print(f"     Reasons: {', '.join(similar['similarity_reasons'][:2])}")
        
    except FileNotFoundError:
        print(f"Results file not found at {results_file}")
    except Exception as e:
        print(f"Error loading results: {e}")


if __name__ == "__main__":
    main()
    
    # Uncomment to analyze results after running main()
    # load_and_analyze_results('../data/prod/gati_shakti_similar_issues_analysis.json')