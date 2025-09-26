import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
import warnings
warnings.filterwarnings('ignore')

# Set up plotting style
plt.style.use('default')
sns.set_palette("husl")

class GatiShaktiFeatureEDA:
    """
    Comprehensive EDA for Gati Shakti Feature Engineering Dataset
    Analyzes traditional, manual, and clustering features
    """
    
    def __init__(self, json_file_path):
        self.json_file_path = json_file_path
        self.df = None
        self.feature_columns = None
        self.traditional_features = None
        self.manual_features = None
        self.clustering_features = None
        
    def load_data(self):
        """Load JSON data and convert to pandas DataFrame"""
        print("="*80)
        print("LOADING GATI SHAKTI FEATURE ENGINEERING DATASET")
        print("="*80)
        
        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self.df = pd.DataFrame(data)
            print(f" Successfully loaded {len(self.df)} records")
            print(f" Total columns: {len(self.df.columns)}")
            
            # Identify feature columns
            self.feature_columns = [col for col in self.df.columns if col.startswith('feat_')]
            self.traditional_features = [col for col in self.feature_columns 
                                       if not col.startswith('feat_manual_') and not col.startswith('feat_clustering_')]
            self.manual_features = [col for col in self.feature_columns if col.startswith('feat_manual_')]
            self.clustering_features = [col for col in self.feature_columns if col.startswith('feat_clustering_')]
            
            print(f" Feature columns identified: {len(self.feature_columns)}")
            print(f"  - Traditional features: {len(self.traditional_features)}")
            print(f"  - Manual features: {len(self.manual_features)}")
            print(f"  - Clustering features: {len(self.clustering_features)}")
            
            return True
            
        except FileNotFoundError:
            print(f" Error: File not found at {self.json_file_path}")
            return False
        except json.JSONDecodeError:
            print(f" Error: Invalid JSON format in {self.json_file_path}")
            return False
        except Exception as e:
            print(f" Error loading data: {e}")
            return False
    
    def basic_dataset_info(self):
        """Display basic dataset information"""
        print("\n" + "="*80)
        print("BASIC DATASET INFORMATION")
        print("="*80)
        
        print(f"Dataset Shape: {self.df.shape}")
        print(f"Memory Usage: {self.df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        # Data types
        print(f"\nData Types:")
        dtype_counts = self.df.dtypes.value_counts()
        for dtype, count in dtype_counts.items():
            print(f"  {dtype}: {count} columns")
        
        # Missing values
        missing_data = self.df.isnull().sum()
        missing_percent = (missing_data / len(self.df)) * 100
        missing_summary = pd.DataFrame({
            'Missing_Count': missing_data,
            'Missing_Percent': missing_percent
        }).sort_values('Missing_Percent', ascending=False)
        
        print(f"\nTop 10 Columns with Missing Values:")
        print(missing_summary.head(10))
        
        # Feature completeness
        feature_missing = self.df[self.feature_columns].isnull().sum()
        feature_completeness = ((len(self.df) - feature_missing) / len(self.df)) * 100
        
        print(f"\nFeature Completeness Summary:")
        print(f"  Average completeness: {feature_completeness.mean():.2f}%")
        print(f"  Min completeness: {feature_completeness.min():.2f}%")
        print(f"  Max completeness: {feature_completeness.max():.2f}%")
        
        # Features with low completeness
        low_completeness = feature_completeness[feature_completeness < 50]
        if len(low_completeness) > 0:
            print(f"\nFeatures with <50% completeness ({len(low_completeness)} features):")
            for feat, completeness in low_completeness.items():
                print(f"  {feat}: {completeness:.1f}%")
    
    def analyze_traditional_features(self):
        """Analyze traditional features (categorical, numerical, date-based)"""
        print("\n" + "="*80)
        print("TRADITIONAL FEATURES ANALYSIS")
        print("="*80)
        
        if not self.traditional_features:
            print("No traditional features found.")
            return
        
        print(f"Analyzing {len(self.traditional_features)} traditional features...")
        
        # Categorize traditional features
        categorical_features = []
        numerical_features = []
        date_features = []
        text_features = []
        
        for feature in self.traditional_features:
            if self.df[feature].dtype == 'object':
                # Check if it's a date feature
                if any(word in feature.lower() for word in ['date', 'time', 'year', 'month', 'quarter']):
                    date_features.append(feature)
                # Check if it's a text feature
                elif any(word in feature.lower() for word in ['text', 'description', 'title']):
                    text_features.append(feature)
                else:
                    categorical_features.append(feature)
            else:
                numerical_features.append(feature)
        
        print(f"\nFeature Categories:")
        print(f"  Categorical: {len(categorical_features)}")
        print(f"  Numerical: {len(numerical_features)}")
        print(f"  Date-based: {len(date_features)}")
        print(f"  Text-based: {len(text_features)}")
        
        # Analyze categorical features
        if categorical_features:
            print(f"\n" + "-"*60)
            print("CATEGORICAL FEATURES ANALYSIS")
            print("-"*60)
            
            for feature in categorical_features[:10]:  # Show top 10
                if self.df[feature].notna().sum() > 0:
                    value_counts = self.df[feature].value_counts()
                    print(f"\n{feature}:")
                    print(f"  Unique values: {self.df[feature].nunique()}")
                    print(f"  Most common: {value_counts.index[0]} ({value_counts.iloc[0]} records)")
                    if len(value_counts) > 1:
                        print(f"  Second most: {value_counts.index[1]} ({value_counts.iloc[1]} records)")
                    print(f"  Completeness: {self.df[feature].notna().sum()}/{len(self.df)} ({self.df[feature].notna().sum()/len(self.df)*100:.1f}%)")
        
        # Analyze numerical features
        if numerical_features:
            print(f"\n" + "-"*60)
            print("NUMERICAL FEATURES ANALYSIS")
            print("-"*60)
            
            numerical_df = self.df[numerical_features]
            print(f"\nNumerical Features Summary:")
            print(numerical_df.describe())
            
            # Identify features with interesting distributions
            print(f"\nKey Numerical Features Insights:")
            for feature in numerical_features:
                if self.df[feature].notna().sum() > 0:
                    non_zero_count = (self.df[feature] != 0).sum()
                    if non_zero_count > 0:
                        print(f"  {feature}:")
                        print(f"    Non-zero values: {non_zero_count}/{len(self.df)} ({non_zero_count/len(self.df)*100:.1f}%)")
                        print(f"    Range: {self.df[feature].min():.2f} to {self.df[feature].max():.2f}")
                        print(f"    Mean: {self.df[feature].mean():.2f}")
        
        # Analyze date features
        if date_features:
            print(f"\n" + "-"*60)
            print("DATE FEATURES ANALYSIS")
            print("-"*60)
            
            for feature in date_features:
                if self.df[feature].notna().sum() > 0:
                    print(f"\n{feature}:")
                    print(f"  Completeness: {self.df[feature].notna().sum()}/{len(self.df)} ({self.df[feature].notna().sum()/len(self.df)*100:.1f}%)")
                    if self.df[feature].dtype == 'object':
                        # Try to identify date patterns
                        sample_dates = self.df[feature].dropna().head(5).tolist()
                        print(f"  Sample values: {sample_dates}")
    
    def analyze_manual_features(self):
        """Analyze the 14 manual features created using LLM"""
        print("\n" + "="*80)
        print("MANUAL FEATURES ANALYSIS (14 LLM-Generated Features)")
        print("="*80)
        
        if not self.manual_features:
            print("No manual features found.")
            return
        
        print(f"Analyzing {len(self.manual_features)} manual features...")
        
        # Feature definitions based on the feature engineering code
        manual_feature_definitions = {
            'feat_manual_issue_type': 'Primary Issue Type (LAND_ACQUISITION, REGULATORY_APPROVALS, etc.)',
            'feat_manual_stakeholder_complexity': 'Stakeholder Complexity Score (1-4)',
            'feat_manual_resolution_strategy': 'Resolution Strategy Type (Administrative, Negotiation, etc.)',
            'feat_manual_timeline_criticality': 'Timeline Criticality (Low, Medium, High, Critical)',
            'feat_manual_geographic_scope': 'Geographic Impact Scope (Local, Regional, State, etc.)',
            'feat_manual_bureaucratic_complexity': 'Bureaucratic Complexity Level (Low, Medium, High, Very_High)',
            'feat_manual_environmental_impact': 'Environmental Impact Category (None, Minimal, Moderate, etc.)',
            'feat_manual_financial_complexity': 'Financial Complexity Level (Low, Medium, High, Very_High)',
            'feat_manual_seasonal_dependency': 'Seasonal Work Dependency (Monsoon_Dependent, Year_Round, etc.)',
            'feat_manual_public_sensitivity': 'Public Interest Sensitivity (Low, Medium, High, Very_High)',
            'feat_manual_legal_risk': 'Legal Risk Assessment (Low, Medium, High, Very_High)',
            'feat_manual_tech_dependency': 'Technology Dependency (No_Tech_Dependency, Standard_IT_Systems, etc.)',
            'feat_manual_infrastructure_type': 'Infrastructure Type Specificity (Linear_Infrastructure, etc.)',
            'feat_manual_displacement_scale': 'Community Displacement Scale (No_Displacement, Minor_Displacement, etc.)'
        }
        
        # Analyze each manual feature
        for feature in self.manual_features:
            if feature in self.df.columns and self.df[feature].notna().sum() > 0:
                print(f"\n" + "-"*60)
                print(f"{feature.upper()}")
                print(f"Definition: {manual_feature_definitions.get(feature, 'N/A')}")
                print("-"*60)
                
                value_counts = self.df[feature].value_counts()
                completeness = self.df[feature].notna().sum() / len(self.df) * 100
                
                print(f"Completeness: {completeness:.1f}%")
                print(f"Unique values: {self.df[feature].nunique()}")
                print(f"Distribution:")
                
                for value, count in value_counts.head(10).items():
                    percentage = (count / len(self.df)) * 100
                    print(f"  {value}: {count} ({percentage:.1f}%)")
                
                if len(value_counts) > 10:
                    print(f"  ... and {len(value_counts) - 10} more categories")
        
        # Cross-feature analysis
        print(f"\n" + "-"*60)
        print("MANUAL FEATURES CROSS-ANALYSIS")
        print("-"*60)
        
        # Analyze stakeholder complexity vs other features
        if 'feat_manual_stakeholder_complexity' in self.df.columns:
            print(f"\nStakeholder Complexity Distribution:")
            stakeholder_dist = self.df['feat_manual_stakeholder_complexity'].value_counts().sort_index()
            for complexity, count in stakeholder_dist.items():
                percentage = (count / len(self.df)) * 100
                print(f"  Level {complexity}: {count} ({percentage:.1f}%)")
        
        # Analyze issue type distribution
        if 'feat_manual_issue_type' in self.df.columns:
            print(f"\nIssue Type Distribution:")
            issue_type_dist = self.df['feat_manual_issue_type'].value_counts()
            for issue_type, count in issue_type_dist.items():
                percentage = (count / len(self.df)) * 100
                print(f"  {issue_type}: {count} ({percentage:.1f}%)")
        
        # Analyze timeline criticality
        if 'feat_manual_timeline_criticality' in self.df.columns:
            print(f"\nTimeline Criticality Distribution:")
            timeline_dist = self.df['feat_manual_timeline_criticality'].value_counts()
            for criticality, count in timeline_dist.items():
                percentage = (count / len(self.df)) * 100
                print(f"  {criticality}: {count} ({percentage:.1f}%)")
    
    def analyze_clustering_features(self):
        """Analyze clustering features for pattern recognition"""
        print("\n" + "="*80)
        print("CLUSTERING FEATURES ANALYSIS")
        print("="*80)
        
        if not self.clustering_features:
            print("No clustering features found.")
            return
        
        print(f"Analyzing {len(self.clustering_features)} clustering features...")
        
        # Feature definitions for clustering features
        clustering_feature_definitions = {
            'feat_clustering_issue_pattern': 'Issue Pattern Classification (Approval_Bottleneck, Resource_Constraint, etc.)',
            'feat_clustering_root_cause': 'Problem Root Cause Analysis (Process_Inefficiency, Information_Gap, etc.)',
            'feat_clustering_solution_approach': 'Solution Approach Categorization (Administrative_Action, Policy_Change, etc.)',
            'feat_clustering_recurrence_pattern': 'Issue Recurrence Pattern (Common_Recurring, Sector_Specific, etc.)',
            'feat_clustering_stakeholder_pattern': 'Stakeholder Involvement Pattern (Single_Authority, Hierarchical_Chain, etc.)',
            'feat_clustering_communication_urgency': 'Communication Urgency Level (Routine, Elevated, High_Priority, Crisis_Mode)',
            'feat_clustering_evolution_stage': 'Issue Evolution Stage (Initial_Identification, Analysis_Phase, etc.)'
        }
        
        # Analyze each clustering feature
        for feature in self.clustering_features:
            if feature in self.df.columns and self.df[feature].notna().sum() > 0:
                print(f"\n" + "-"*60)
                print(f"{feature.upper()}")
                print(f"Definition: {clustering_feature_definitions.get(feature, 'N/A')}")
                print("-"*60)
                
                value_counts = self.df[feature].value_counts()
                completeness = self.df[feature].notna().sum() / len(self.df) * 100
                
                print(f"Completeness: {completeness:.1f}%")
                print(f"Unique values: {self.df[feature].nunique()}")
                print(f"Distribution:")
                
                for value, count in value_counts.head(10).items():
                    percentage = (count / len(self.df)) * 100
                    print(f"  {value}: {count} ({percentage:.1f}%)")
                
                if len(value_counts) > 10:
                    print(f"  ... and {len(value_counts) - 10} more categories")
        
        # Clustering readiness analysis
        print(f"\n" + "-"*60)
        print("CLUSTERING READINESS ANALYSIS")
        print("-"*60)
        
        # Check for balanced distributions
        balanced_features = []
        skewed_features = []
        
        for feature in self.clustering_features:
            if feature in self.df.columns and self.df[feature].notna().sum() > 0:
                value_counts = self.df[feature].value_counts()
                max_count = value_counts.max()
                min_count = value_counts.min()
                balance_ratio = min_count / max_count if max_count > 0 else 0
                
                if balance_ratio > 0.1:  # Consider balanced if smallest category is >10% of largest
                    balanced_features.append(feature)
                else:
                    skewed_features.append(feature)
        
        print(f"Balanced features (good for clustering): {len(balanced_features)}")
        for feature in balanced_features:
            print(f"   {feature}")
        
        print(f"\nSkewed features (may need attention): {len(skewed_features)}")
        for feature in skewed_features:
            print(f"   {feature}")
    
    def feature_richness_analysis(self):
        """Analyze the richness and quality of features"""
        print("\n" + "="*80)
        print("FEATURE RICHNESS & QUALITY ANALYSIS")
        print("="*80)
        
        # Calculate feature richness metrics
        richness_metrics = {}
        
        for feature in self.feature_columns:
            if feature in self.df.columns:
                # Completeness
                completeness = self.df[feature].notna().sum() / len(self.df)
                
                # Diversity (for categorical features)
                if self.df[feature].dtype == 'object':
                    diversity = self.df[feature].nunique() / len(self.df)
                else:
                    # For numerical features, use coefficient of variation
                    if self.df[feature].std() > 0:
                        diversity = self.df[feature].std() / abs(self.df[feature].mean()) if self.df[feature].mean() != 0 else 0
                    else:
                        diversity = 0
                
                # Information content (entropy for categorical)
                if self.df[feature].dtype == 'object':
                    value_counts = self.df[feature].value_counts()
                    probabilities = value_counts / value_counts.sum()
                    entropy = -sum(p * np.log2(p) for p in probabilities if p > 0)
                    max_entropy = np.log2(len(value_counts)) if len(value_counts) > 1 else 0
                    information_content = entropy / max_entropy if max_entropy > 0 else 0
                else:
                    information_content = min(diversity, 1.0)  # Normalize
                
                richness_metrics[feature] = {
                    'completeness': completeness,
                    'diversity': diversity,
                    'information_content': information_content,
                    'richness_score': (completeness + diversity + information_content) / 3
                }
        
        # Sort by richness score
        sorted_features = sorted(richness_metrics.items(), key=lambda x: x[1]['richness_score'], reverse=True)
        
        print(f"Top 20 Most Rich Features:")
        print(f"{'Feature':<40} {'Completeness':<12} {'Diversity':<10} {'Info_Content':<12} {'Richness':<10}")
        print("-" * 90)
        
        for feature, metrics in sorted_features[:20]:
            print(f"{feature:<40} {metrics['completeness']:<12.3f} {metrics['diversity']:<10.3f} "
                  f"{metrics['information_content']:<12.3f} {metrics['richness_score']:<10.3f}")
        
        # Feature category analysis
        print(f"\n" + "-"*60)
        print("FEATURE CATEGORY RICHNESS")
        print("-"*60)
        
        categories = {
            'Traditional': self.traditional_features,
            'Manual': self.manual_features,
            'Clustering': self.clustering_features
        }
        
        for category, features in categories.items():
            if features:
                category_metrics = [richness_metrics[f] for f in features if f in richness_metrics]
                if category_metrics:
                    avg_completeness = np.mean([m['completeness'] for m in category_metrics])
                    avg_diversity = np.mean([m['diversity'] for m in category_metrics])
                    avg_info_content = np.mean([m['information_content'] for m in category_metrics])
                    avg_richness = np.mean([m['richness_score'] for m in category_metrics])
                    
                    print(f"{category} Features ({len(features)} features):")
                    print(f"  Average Completeness: {avg_completeness:.3f}")
                    print(f"  Average Diversity: {avg_diversity:.3f}")
                    print(f"  Average Information Content: {avg_info_content:.3f}")
                    print(f"  Average Richness Score: {avg_richness:.3f}")
        
        # Classification quality assessment
        print(f"\n" + "-"*60)
        print("CLASSIFICATION QUALITY ASSESSMENT")
        print("-"*60)
        
        # Check for well-distributed categorical features
        well_distributed = []
        poorly_distributed = []
        
        for feature in self.feature_columns:
            if (feature in self.df.columns and 
                self.df[feature].dtype == 'object' and 
                self.df[feature].notna().sum() > 0):
                
                value_counts = self.df[feature].value_counts()
                if len(value_counts) > 1:
                    # Check if distribution is reasonably balanced
                    max_count = value_counts.max()
                    min_count = value_counts.min()
                    balance_ratio = min_count / max_count
                    
                    # Check if no single category dominates (>80%)
                    max_proportion = max_count / len(self.df)
                    
                    if balance_ratio > 0.05 and max_proportion < 0.8:
                        well_distributed.append(feature)
                    else:
                        poorly_distributed.append(feature)
        
        print(f"Well-distributed features (good for classification): {len(well_distributed)}")
        for feature in well_distributed[:10]:  # Show top 10
            print(f"   {feature}")
        
        print(f"\nPoorly-distributed features (may need attention): {len(poorly_distributed)}")
        for feature in poorly_distributed[:10]:  # Show top 10
            print(f"   {feature}")
    
    def generate_visualizations(self):
        """Generate key visualizations for feature analysis"""
        print("\n" + "="*80)
        print("GENERATING VISUALIZATIONS")
        print("="*80)
        
        try:
            # Set up the plotting area
            fig = plt.figure(figsize=(20, 24))
            
            # 1. Feature completeness heatmap
            plt.subplot(4, 2, 1)
            feature_completeness = self.df[self.feature_columns].notna().sum() / len(self.df)
            feature_completeness = feature_completeness.sort_values(ascending=True)
            
            # Create a simple bar plot for top 20 features
            top_features = feature_completeness.tail(20)
            plt.barh(range(len(top_features)), top_features.values)
            plt.yticks(range(len(top_features)), [f.replace('feat_', '') for f in top_features.index], fontsize=8)
            plt.xlabel('Completeness Ratio')
            plt.title('Feature Completeness (Top 20)')
            plt.grid(axis='x', alpha=0.3)
            
            # 2. Manual features distribution
            if self.manual_features:
                plt.subplot(4, 2, 2)
                # Count non-null values for each manual feature
                manual_completeness = [self.df[f].notna().sum() for f in self.manual_features if f in self.df.columns]
                manual_feature_names = [f.replace('feat_manual_', '') for f in self.manual_features if f in self.df.columns]
                
                plt.bar(range(len(manual_completeness)), manual_completeness)
                plt.xticks(range(len(manual_feature_names)), manual_feature_names, rotation=45, ha='right', fontsize=8)
                plt.ylabel('Number of Records')
                plt.title('Manual Features Completeness')
                plt.grid(axis='y', alpha=0.3)
            
            # 3. Issue type distribution (if available)
            if 'feat_manual_issue_type' in self.df.columns:
                plt.subplot(4, 2, 3)
                issue_type_counts = self.df['feat_manual_issue_type'].value_counts()
                plt.pie(issue_type_counts.values, labels=issue_type_counts.index, autopct='%1.1f%%', startangle=90)
                plt.title('Issue Type Distribution')
            
            # 4. Stakeholder complexity distribution
            if 'feat_manual_stakeholder_complexity' in self.df.columns:
                plt.subplot(4, 2, 4)
                stakeholder_counts = self.df['feat_manual_stakeholder_complexity'].value_counts().sort_index()
                plt.bar(stakeholder_counts.index, stakeholder_counts.values)
                plt.xlabel('Complexity Level')
                plt.ylabel('Number of Records')
                plt.title('Stakeholder Complexity Distribution')
                plt.grid(axis='y', alpha=0.3)
            
            # 5. Timeline criticality distribution
            if 'feat_manual_timeline_criticality' in self.df.columns:
                plt.subplot(4, 2, 5)
                timeline_counts = self.df['feat_manual_timeline_criticality'].value_counts()
                plt.bar(range(len(timeline_counts)), timeline_counts.values)
                plt.xticks(range(len(timeline_counts)), timeline_counts.index, rotation=45, ha='right')
                plt.ylabel('Number of Records')
                plt.title('Timeline Criticality Distribution')
                plt.grid(axis='y', alpha=0.3)
            
            # 6. Clustering features completeness
            if self.clustering_features:
                plt.subplot(4, 2, 6)
                clustering_completeness = [self.df[f].notna().sum() for f in self.clustering_features if f in self.df.columns]
                clustering_feature_names = [f.replace('feat_clustering_', '') for f in self.clustering_features if f in self.df.columns]
                
                plt.bar(range(len(clustering_completeness)), clustering_completeness)
                plt.xticks(range(len(clustering_feature_names)), clustering_feature_names, rotation=45, ha='right', fontsize=8)
                plt.ylabel('Number of Records')
                plt.title('Clustering Features Completeness')
                plt.grid(axis='y', alpha=0.3)
            
            # 7. Feature richness comparison
            plt.subplot(4, 2, 7)
            categories = ['Traditional', 'Manual', 'Clustering']
            counts = [len(self.traditional_features), len(self.manual_features), len(self.clustering_features)]
            colors = ['skyblue', 'lightcoral', 'lightgreen']
            
            plt.bar(categories, counts, color=colors)
            plt.ylabel('Number of Features')
            plt.title('Feature Categories Distribution')
            plt.grid(axis='y', alpha=0.3)
            
            # Add count labels on bars
            for i, count in enumerate(counts):
                plt.text(i, count + 0.1, str(count), ha='center', va='bottom')
            
            # 8. Data types distribution
            plt.subplot(4, 2, 8)
            feature_dtypes = self.df[self.feature_columns].dtypes.value_counts()
            plt.pie(feature_dtypes.values, labels=feature_dtypes.index, autopct='%1.1f%%', startangle=90)
            plt.title('Feature Data Types Distribution')
            
            plt.tight_layout()
            plt.savefig('gati_shakti_feature_eda_visualizations.png', dpi=300, bbox_inches='tight')
            print(" Visualizations saved as 'gati_shakti_feature_eda_visualizations.png'")
            
        except Exception as e:
            print(f" Error generating visualizations: {e}")
    
    def generate_summary_report(self):
        """Generate a comprehensive summary report"""
        print("\n" + "="*80)
        print("COMPREHENSIVE SUMMARY REPORT")
        print("="*80)
        
        # Dataset overview
        print(f" DATASET OVERVIEW")
        print(f"   Total Records: {len(self.df):,}")
        print(f"   Total Features: {len(self.feature_columns)}")
        print(f"   Traditional Features: {len(self.traditional_features)}")
        print(f"   Manual Features: {len(self.manual_features)}")
        print(f"   Clustering Features: {len(self.clustering_features)}")
        
        # Feature quality summary
        feature_completeness = self.df[self.feature_columns].notna().sum() / len(self.df)
        avg_completeness = feature_completeness.mean()
        
        print(f"\n FEATURE QUALITY")
        print(f"   Average Completeness: {avg_completeness:.1%}")
        print(f"   High Quality Features (>90% complete): {(feature_completeness > 0.9).sum()}")
        print(f"   Medium Quality Features (50-90% complete): {((feature_completeness >= 0.5) & (feature_completeness <= 0.9)).sum()}")
        print(f"   Low Quality Features (<50% complete): {(feature_completeness < 0.5).sum()}")
        
        # Manual features analysis
        if self.manual_features:
            print(f"\n MANUAL FEATURES (LLM-Generated)")
            manual_complete = sum(1 for f in self.manual_features if f in self.df.columns and self.df[f].notna().sum() > 0)
            print(f"   Successfully Generated: {manual_complete}/{len(self.manual_features)}")
            
            if 'feat_manual_issue_type' in self.df.columns:
                issue_types = self.df['feat_manual_issue_type'].value_counts()
                print(f"   Most Common Issue Type: {issue_types.index[0]} ({issue_types.iloc[0]} records)")
            
            if 'feat_manual_stakeholder_complexity' in self.df.columns:
                avg_complexity = self.df['feat_manual_stakeholder_complexity'].mean()
                print(f"   Average Stakeholder Complexity: {avg_complexity:.2f}/4")
        
        # Clustering readiness
        if self.clustering_features:
            print(f"\n CLUSTERING READINESS")
            clustering_complete = sum(1 for f in self.clustering_features if f in self.df.columns and self.df[f].notna().sum() > 0)
            print(f"   Clustering Features Available: {clustering_complete}/{len(self.clustering_features)}")
            
            if 'feat_clustering_issue_pattern' in self.df.columns:
                patterns = self.df['feat_clustering_issue_pattern'].value_counts()
                print(f"   Most Common Issue Pattern: {patterns.index[0]} ({patterns.iloc[0]} records)")
        
        # Recommendations
        print(f"\n RECOMMENDATIONS")
        
        if avg_completeness < 0.8:
            print(f"     Consider data cleaning for features with low completeness")
        
        if len(self.manual_features) > 0:
            print(f"    Manual features provide rich categorical information for clustering")
        
        if len(self.clustering_features) > 0:
            print(f"    Clustering features enable pattern-based grouping")
        
        print(f"    Dataset is ready for similarity analysis and clustering")
        print(f"    Consider using manual features for primary clustering")
        print(f"    Use clustering features for sub-pattern identification")
        
        # Feature engineering success metrics
        print(f"\n FEATURE ENGINEERING SUCCESS METRICS")
        print(f"   Feature Coverage: {len(self.feature_columns)}/{len(self.df.columns)} ({len(self.feature_columns)/len(self.df.columns)*100:.1f}%)")
        print(f"   LLM Feature Success Rate: {manual_complete/len(self.manual_features)*100:.1f}%" if self.manual_features else "   LLM Features: N/A")
        print(f"   Clustering Feature Success Rate: {clustering_complete/len(self.clustering_features)*100:.1f}%" if self.clustering_features else "   Clustering Features: N/A")
        
        print(f"\n" + "="*80)
        print("EDA ANALYSIS COMPLETE")
        print("="*80)

def main():
    """Main function to run the EDA analysis"""
    # Initialize EDA analyzer
    json_file_path = "data/prod/gati_shakti_feature_engineering.json"
    eda = GatiShaktiFeatureEDA(json_file_path)
    
    # Load data
    if not eda.load_data():
        return
    
    # Run comprehensive analysis
    eda.basic_dataset_info()
    eda.analyze_traditional_features()
    eda.analyze_manual_features()
    eda.analyze_clustering_features()
    eda.feature_richness_analysis()
    eda.generate_visualizations()
    eda.generate_summary_report()

if __name__ == "__main__":
    main()
