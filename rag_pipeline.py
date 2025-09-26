import json
import os
from typing import List, Dict, Any
from pathlib import Path

from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain.schema import Document

class RAGPipeline:
    def __init__(self, data_dir: str = "api_response", persist_directory: str = "vector_store"):
        self.data_dir = Path(data_dir)
        self.persist_directory = persist_directory
        self.vectorstore = None
        self.embeddings = HuggingFaceEmbeddings(
            model_name="sentence-transformers/all-MiniLM-L6-v2"
        )
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=200,
            length_function=len,
        )
    
    def load_json_data(self) -> List[Dict[str, Any]]:
        """Load and combine all JSON data files"""
        combined_data = []
        
        json_files = [
            "get_issue_data.json",
            "get_meeting_decision.json", 
            "project_data.json",
            "New_Request-1758087105470.json"
        ]
        
        for file_name in json_files:

            # check for the file paths
            file_path = self.data_dir / file_name
            if file_path.exists():
                print(f"Loading {file_name}...")
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        # Add source information
                        if isinstance(data, dict) and "data" in data:
                            for item in data["data"]:
                                item["_source"] = file_name
                                
                                item["_data_type"] = self._get_data_type(file_name)
                        combined_data.append(data)
                except Exception as e:
                    print(f"Error loading {file_name}: {e}")
            else:
                print(f"File {file_name} not found")
        
        return combined_data
    
    def _get_data_type(self, file_name: str) -> str:
        """Determine data type based on filename"""
        if "issue" in file_name.lower():
            return "issue_data"
        elif "meeting" in file_name.lower():
            return "meeting_decision"
        elif "project" in file_name.lower():
            return "project_data"
        else:
            return "request_data"
    
    def create_documents(self, data: List[Dict[str, Any]]) -> List[Document]:
        """Convert JSON data to LangChain Documents"""
        documents = []
        
        for dataset in data:
            if isinstance(dataset, dict) and "data" in dataset:
                for item in dataset["data"]:
                    # Create a comprehensive text representation
                    text_content = self._create_text_content(item)
                    
                    # Create metadata
                    metadata = {
                        "source": item.get("_source", "unknown"),
                        "data_type": item.get("_data_type", "unknown"),
                        "project_id": item.get("projectId", "unknown"),
                        "issue_id": item.get("issueId", "unknown"),
                    }
                    
                    # Add specific metadata based on data type
                    if item.get("_data_type") == "project_data":
                        metadata.update({
                            "project_name": item.get("projectName", ""),
                            "sector": item.get("sector", ""),
                            "location": ", ".join(item.get("locations", [])),
                            "status": item.get("projectStatus", ""),
                        })
                    elif item.get("_data_type") == "meeting_decision":
                        metadata.update({
                            "meeting_name": item.get("meetingName", ""),
                            "meeting_date": item.get("meetingDate", ""),
                            "meeting_type": item.get("meetingType", ""),
                        })
                    
                    doc = Document(page_content=text_content, metadata=metadata)
                    documents.append(doc)
        
        return documents
    
    def _create_text_content(self, item: Dict[str, Any]) -> str:
        """Create a comprehensive text representation of the data item"""
        text_parts = []
        
        # Add basic information
        if "projectName" in item:
            text_parts.append(f"Project: {item['projectName']}")
        if "projectDescription" in item:
            text_parts.append(f"Description: {item['projectDescription']}")
        if "sector" in item:
            text_parts.append(f"Sector: {item['sector']}")
        if "locations" in item:
            text_parts.append(f"Locations: {', '.join(item['locations'])}")
        
        # Add issue information
        if "issueDescription" in item:
            text_parts.append(f"Issue: {item['issueDescription']}")
        if "issueStatus" in item:
            text_parts.append(f"Issue Status: {item['issueStatus']}")
        
        # Add meeting decision information
        if "meetingDecision" in item:
            text_parts.append(f"Meeting Decision: {item['meetingDecision']}")
        if "meetingName" in item:
            text_parts.append(f"Meeting: {item['meetingName']}")
        if "meetingDate" in item:
            text_parts.append(f"Meeting Date: {item['meetingDate']}")
        
        # Add project status and progress
        if "projectStatus" in item:
            text_parts.append(f"Project Status: {item['projectStatus']}")
        if "actualPhysicalProgress" in item:
            text_parts.append(f"Physical Progress: {item['actualPhysicalProgress']}%")
        if "actualFinancialProgress" in item:
            text_parts.append(f"Financial Progress: {item['actualFinancialProgress']}%")
        
        # Add cost information
        if "projectCost" in item:
            text_parts.append(f"Project Cost: {item['projectCost']} crores")
        
        # Add decisions if available
        if "decisions" in item and isinstance(item["decisions"], list):
            for decision in item["decisions"]:
                if isinstance(decision, dict):
                    decision_text = f"Decision: {decision.get('meetingDecision', '')}"
                    if decision.get("meetingDate"):
                        decision_text += f" (Date: {decision['meetingDate']})"
                    text_parts.append(decision_text)
        
        return " | ".join(text_parts)
    
    def build_vector_store(self):
        """Build the vector store from JSON data"""
        print("Loading JSON data...")
        data = self.load_json_data()
        
        print("Creating documents...")
        documents = self.create_documents(data)
        
        print(f"Created {len(documents)} documents")
        
        print("Splitting documents into chunks...")
        split_documents = self.text_splitter.split_documents(documents)
        
        print(f"Split into {len(split_documents)} chunks")
        
        print("Creating vector store...")
        self.vectorstore = Chroma.from_documents(
            documents=split_documents,
            embedding=self.embeddings,
            persist_directory=self.persist_directory
        )
        
        print(f"Vector store created and saved to {self.persist_directory}")
    
    def load_vector_store(self):
        """Load existing vector store"""
        if os.path.exists(self.persist_directory):
            print("Loading existing vector store...")
            self.vectorstore = Chroma(
                persist_directory=self.persist_directory,
                embedding_function=self.embeddings
            )
            print("Vector store loaded successfully")
        else:
            print("No existing vector store found. Building new one...")
            self.build_vector_store()
    
    def search(self, query: str, k: int = 5) -> List[Document]:
        """Search the vector store"""
        if self.vectorstore is None:
            self.load_vector_store()
        
        results = self.vectorstore.similarity_search(query, k=k)
        return results
    
    def search_with_scores(self, query: str, k: int = 5) -> List[tuple]:
        """Search the vector store with similarity scores"""
        if self.vectorstore is None:
            self.load_vector_store()
        
        results = self.vectorstore.similarity_search_with_score(query, k=k)
        return results

def main():
    """Main function to run the RAG pipeline"""
    print("Starting RAG Pipeline...")
    
    # Initialize the pipeline
    rag_pipeline = RAGPipeline()
    
    # Build or load the vector store
    rag_pipeline.load_vector_store()
    
    # Example search queries
    example_queries = [
        "railway projects in Uttar Pradesh",
        "land acquisition issues",
        "meeting decisions about project delays",
        "projects with cost overruns",
        "infrastructure projects in Maharashtra"
    ]
    
    print("\n" + "="*50)
    print("Example Search Results:")
    print("="*50)
    
    for query in example_queries:
        print(f"\nQuery: {query}")
        print("-" * 30)
        
        results = rag_pipeline.search(query, k=3)
        
        for i, doc in enumerate(results, 1):
            print(f"\nResult {i}:")
            print(f"Source: {doc.metadata.get('source', 'unknown')}")
            print(f"Data Type: {doc.metadata.get('data_type', 'unknown')}")
            if doc.metadata.get("project_name"):
                print(f"Project: {doc.metadata['project_name']}")
            print(f"Content: {doc.page_content[:200]}...")
    
    print("\n" + "="*50)
    print("RAG Pipeline completed successfully!")
    print("Vector store is ready for use.")

if __name__ == "__main__":
    main()
