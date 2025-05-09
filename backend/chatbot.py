import os
import uuid
import time
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import snowflake.connector
import argparse
import logging
from pathlib import Path
import json
import csv
import io
import threading
import tempfile
import shutil

# Updated LangChain imports
from langchain_google_genai import GoogleGenerativeAIEmbeddings, ChatGoogleGenerativeAI
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_community.vectorstores import FAISS
from langchain_community.document_loaders import CSVLoader
from langchain_core.documents import Document
from langchain_huggingface import HuggingFaceEmbeddings

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set environment variables (These would typically be stored securely)
  # Replace with your actual API key

# Snowflake configuration
SNOWFLAKE_CONFIG = {
    "account":"MAOOPFN-RN95859",
    "user":"SAMYUKTHA",
    "password":"9ZZGvzRG7pn8T66",
    "warehouse":"reddit_wh",
    "database":"reddit_feedback_db",
    "schema":"reddit_schema"
}

# Data storage paths
VECTOR_STORE_DIRECTORY = Path("./faiss_index")
VECTOR_STORE_DIRECTORY.mkdir(exist_ok=True, parents=True)
CSV_DIRECTORY = Path("./csv_data")
CSV_DIRECTORY.mkdir(exist_ok=True, parents=True)
METADATA_DIRECTORY = Path("./metadata")
METADATA_DIRECTORY.mkdir(exist_ok=True, parents=True)

# Initialize embeddings model
def get_embeddings_model():
    """Initialize a HuggingFace embeddings model that's free to use"""
    model_name = "all-MiniLM-L6-v2"  # This is a small, fast model that works well
    model_kwargs = {'device': 'cpu'}  # Use CPU to avoid CUDA issues
    encode_kwargs = {'normalize_embeddings': True}
    
    return HuggingFaceEmbeddings(
        model_name=model_name,
        model_kwargs=model_kwargs,
        encode_kwargs=encode_kwargs
    )

# Initialize LLM
def get_llm():
   
    return ChatGoogleGenerativeAI(model="gemini-1.5-flash", temperature=0.3,google_api_key="AIzaSyDTpDeQpy6Ql6s1Ug6HsZId5jACVPNwJJI")

class CustomerInsightsChatbot:
    def __init__(self, auto_init=False):
        self.embeddings = get_embeddings_model()
        self.llm = get_llm()
        self.vector_store = None
        self.retriever = None
        self.chain = None
        self.last_update_time = None
        self.documents_metadata = {}
        
        # Lock for thread safety during updates
        self.update_lock = threading.Lock()
        
        # Initialize vector store if it exists
        self._initialize_vector_store()
        
        # Automatically load data from Snowflake if auto_init is True
        if auto_init:
            self._auto_initialize()
    
    def _auto_initialize(self):
        """Automatically initialize with Snowflake data if vector store is empty"""
        if self.vector_store is None or len(self.documents_metadata) <= 1:  # Only placeholder document
            try:
                logger.info("Auto-initializing with data from Snowflake...")
                df = self.fetch_snowflake_data()
                if not df.empty:
                    documents = self.process_snowflake_data(df)
                    self.add_documents_to_vector_store(documents)
                    logger.info(f"Auto-initialization complete: Added {len(documents)} documents from Snowflake")
                else:
                    logger.warning("No data found in Snowflake for auto-initialization")
            except Exception as e:
                logger.error(f"Error during auto-initialization: {e}")
                
    def _initialize_vector_store(self):
        """Initialize the vector store if it exists, create it otherwise"""
        if VECTOR_STORE_DIRECTORY.exists() and any(VECTOR_STORE_DIRECTORY.iterdir()):
            logger.info("Loading existing FAISS vector store...")
            try:
                self.vector_store = FAISS.load_local(
                    str(VECTOR_STORE_DIRECTORY), 
                    self.embeddings
                )
                self.retriever = self.vector_store.as_retriever(
                    search_type="similarity",
                    search_kwargs={"k": 5}
                )
                self._create_chain()
                
                # Load metadata if available
                self._load_metadata()
                
                # Load the last update time
                self._load_update_time()
                
                doc_count = len(self.documents_metadata) if self.documents_metadata else "unknown number of"
                logger.info(f"Vector store loaded with {doc_count} documents")
            except Exception as e:
                logger.error(f"Error loading existing vector store: {e}")
                logger.info("Creating a new vector store...")
                self._create_empty_vector_store()
        else:
            logger.info("No existing vector store found. Creating a new one...")
            self._create_empty_vector_store()
            
    def _create_empty_vector_store(self):
        """Create an empty FAISS vector store with a placeholder document"""
        try:
            # Create an empty vector store with a placeholder document
            placeholder_doc = Document(
                page_content="This is a placeholder document for initialization",
                metadata={"source": "placeholder"}
            )
            
            self.vector_store = FAISS.from_documents(
                [placeholder_doc],
                self.embeddings
            )
            
            # Save immediately to establish the directory structure
            self.vector_store.save_local(str(VECTOR_STORE_DIRECTORY))
            
            # Set up retriever
            self.retriever = self.vector_store.as_retriever(
                search_type="similarity",
                search_kwargs={"k": 5}
            )
            
            # Create chain
            self._create_chain()
            
            # Initialize metadata
            self.documents_metadata = {
                "placeholder": {"source": "placeholder", "initialized_at": datetime.now().isoformat()}
            }
            self._save_metadata()
            
            logger.info("Created new empty vector store with placeholder document")
        except Exception as e:
            logger.error(f"Error creating empty vector store: {e}")
            raise
    
    def _save_metadata(self):
        """Save document metadata to disk"""
        try:
            metadata_path = METADATA_DIRECTORY / "documents_metadata.json"
            with open(metadata_path, "w") as f:
                json.dump(self.documents_metadata, f, indent=2)
            logger.info(f"Saved metadata for {len(self.documents_metadata)} documents")
        except Exception as e:
            logger.error(f"Error saving metadata: {e}")
    
    def _load_metadata(self):
        """Load document metadata from disk"""
        try:
            metadata_path = METADATA_DIRECTORY / "documents_metadata.json"
            if metadata_path.exists():
                with open(metadata_path, "r") as f:
                    self.documents_metadata = json.load(f)
                logger.info(f"Loaded metadata for {len(self.documents_metadata)} documents")
            else:
                self.documents_metadata = {}
                logger.warning("No metadata file found")
        except Exception as e:
            logger.error(f"Error loading metadata: {e}")
            self.documents_metadata = {}
    
    def connect_to_snowflake(self):
        """Establish connection to Snowflake"""
        try:
            conn = snowflake.connector.connect(
                account=SNOWFLAKE_CONFIG["account"],
                user=SNOWFLAKE_CONFIG["user"],
                password=SNOWFLAKE_CONFIG["password"],
                warehouse=SNOWFLAKE_CONFIG["warehouse"],
                database=SNOWFLAKE_CONFIG["database"],
                schema=SNOWFLAKE_CONFIG["schema"]
            )
            logger.info("Successfully connected to Snowflake")
            return conn
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {e}")
            raise
    
    def fetch_snowflake_data(self, last_updated=None):
        """Fetch data from Snowflake with optional last_updated filter"""
        try:
            conn = self.connect_to_snowflake()
            try:
                cursor = conn.cursor()
                
                # Query to fetch customer feedback data
                query = """
                    SELECT * FROM reddit_feedback_final
                """
                
                # Add filter for incremental updates if last_updated is provided
                if last_updated:
                    if isinstance(last_updated, datetime):
                        last_updated_str = last_updated.strftime('%Y-%m-%d %H:%M:%S')
                        query += f" WHERE CREATED_DATE > '{last_updated_str}'"
                
                # Limit query for development/testing to avoid overloading
                # Remove or adjust this limit in production
                query += " LIMIT 1000"
                    
                cursor.execute(query)
                
                # Get column names
                column_names = [desc[0] for desc in cursor.description]
                
                # Fetch all results
                results = cursor.fetchall()
                
                # Convert to DataFrame
                df = pd.DataFrame(results, columns=column_names)
                logger.info(f"Fetched {len(df)} records from Snowflake")
                
                return df
            except Exception as e:
                logger.error(f"Error executing Snowflake query: {e}")
                raise
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Fatal error with Snowflake connection: {e}")
            # Return empty DataFrame instead of raising to allow graceful handling
            return pd.DataFrame()
    
    def process_snowflake_data(self, df):
        """Process Snowflake data into documents for the vector store"""
        documents = []
        
        if df is None or df.empty:
            logger.warning("No data to process from Snowflake")
            return documents
            
        # Assuming the DataFrame has columns like 'FEEDBACK_ID', 'CUSTOMER_ID', 'FEEDBACK_TEXT', etc.
        for index, row in df.iterrows():
            try:
                # Combine relevant fields into content
                content = f"Author: {row.get('AUTHOR', 'N/A')}\n"
                content += f"Subreddit: {row.get('SUBREDDIT', 'N/A')}\n"
                content += f"Post Title: {row.get('POST_TITLE', 'N/A')}\n"
                content += f"Feedback: {row.get('TEXT', '')}\n"
                
                # Add additional relevant fields
                content += f"Date: {row.get('CREATED_DATE', 'N/A')}\n"
                content += f"Score: {row.get('SCORE', 'N/A')}\n"
                content += f"Sentiment: {row.get('SENTIMENT_CATEGORY', 'N/A')}\n"
                
                if 'TOPICS' in row and row['TOPICS']:
                    content += f"Topics: {row.get('TOPICS', 'N/A')}\n"
                
                if 'KEYWORDS' in row and row['KEYWORDS']:
                    content += f"Keywords: {row.get('KEYWORDS', 'N/A')}\n"
                
                # Create metadata
                feedback_id = str(row.get('FEEDBACK_ID', str(uuid.uuid4())))
                metadata = {
                    "source": "snowflake",
                    "feedback_id": feedback_id,
                    "author": str(row.get('AUTHOR', 'unknown')),
                    "created_date": str(row.get('CREATED_DATE', datetime.now())),
                    "subreddit": str(row.get('SUBREDDIT', 'unknown')),
                    "permalink": str(row.get('PERMALINK', 'N/A')),
                }
                
                # Add sentiment if available
                if 'SENTIMENT_CATEGORY' in row:
                    metadata["sentiment"] = str(row.get('SENTIMENT_CATEGORY', 'unknown'))
                
                # Add feedback type if available
                if 'FEEDBACK_TYPE' in row:
                    metadata["feedback_type"] = str(row.get('FEEDBACK_TYPE', 'unknown'))
                
                # Add additional metadata fields if available
                for col in row.index:
                    if col not in ['FEEDBACK_ID', 'AUTHOR', 'CREATED_DATE', 'TEXT', 'SUBREDDIT', 'PERMALINK']:
                        metadata[col.lower()] = str(row.get(col, ''))
                
                doc = Document(page_content=content, metadata=metadata)
                documents.append(doc)
                
                # Store metadata separately for future reference
                self.documents_metadata[feedback_id] = metadata
                
            except Exception as e:
                logger.error(f"Error processing row {index}: {e}")
                continue
            
        return documents
    
    def process_csv_file(self, file_path):
        """Process a CSV file into documents for the vector store"""
        try:
            # First, check if the file exists
            if not Path(file_path).exists():
                logger.error(f"CSV file not found: {file_path}")
                return []
                
            # Try to read the file to validate it's a proper CSV
            try:
                df = pd.read_csv(file_path)
                logger.info(f"Successfully read CSV with {len(df)} rows and {len(df.columns)} columns")
            except Exception as e:
                logger.error(f"Error reading CSV file {file_path}: {e}")
                return []
            
            # Use LangChain's CSVLoader
            loader = CSVLoader(
                file_path=file_path,
                csv_args={
                    'delimiter': ',',
                    'quotechar': '"',
                }
            )
            
            try:
                documents = loader.load()
            except Exception as e:
                logger.error(f"CSVLoader failed: {e}. Falling back to manual processing.")
                # Fallback to manual processing
                documents = []
                for _, row in df.iterrows():
                    content = "\n".join([f"{col}: {val}" for col, val in row.items() if not pd.isna(val)])
                    doc = Document(
                        page_content=content,
                        metadata={"source": f"csv:{Path(file_path).name}"}
                    )
                    documents.append(doc)
            
            # Process documents to add metadata and format content
            processed_docs = []
            for doc in documents:
                # Generate a unique ID for this document
                doc_id = str(uuid.uuid4())
                
                metadata = doc.metadata
                metadata["source"] = "csv"
                metadata["file_name"] = Path(file_path).name
                metadata["doc_id"] = doc_id
                metadata["imported_at"] = datetime.now().isoformat()
                
                processed_doc = Document(
                    page_content=doc.page_content,
                    metadata=metadata
                )
                processed_docs.append(processed_doc)
                
                # Store metadata separately
                self.documents_metadata[doc_id] = metadata
                
            logger.info(f"Processed {len(processed_docs)} documents from CSV file: {file_path}")
            return processed_docs
        except Exception as e:
            logger.error(f"Error processing CSV file {file_path}: {e}")
            return []
    
    def import_csv_files(self, file_paths):
        """Import multiple CSV files into the vector store"""
        all_documents = []
        
        for file_path in file_paths:
            path = Path(file_path)
            if not path.exists():
                logger.warning(f"File does not exist: {file_path}")
                continue
                
            if path.suffix.lower() != '.csv':
                logger.warning(f"Not a CSV file: {file_path}")
                continue
                
            try:
                documents = self.process_csv_file(file_path)
                all_documents.extend(documents)
                
                # Copy the CSV file to the CSV directory for future reference
                target_path = CSV_DIRECTORY / path.name
                if not target_path.exists():
                    shutil.copy2(path, target_path)
                    logger.info(f"Copied CSV file to {target_path}")
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                continue
        
        if all_documents:
            self.add_documents_to_vector_store(all_documents)
            logger.info(f"Added {len(all_documents)} documents from CSV files to vector store")
            return {"status": "success", "message": f"Imported {len(all_documents)} documents from {len(file_paths)} CSV files"}
        else:
            return {"status": "warning", "message": "No documents were successfully imported from CSV files"}
            
    def watch_csv_directory(self):
        """Check for new CSV files in the CSV directory and process them"""
        try:
            processed_files = set()
            
            # Get list of already processed files
            try:
                processed_file_path = CSV_DIRECTORY / ".processed_files.json"
                if processed_file_path.exists():
                    with open(processed_file_path, "r") as f:
                        processed_files = set(json.load(f))
            except Exception as e:
                logger.error(f"Error loading processed files list: {e}")
            
            # Check for new CSV files
            new_files = []
            for file_path in CSV_DIRECTORY.glob("*.csv"):
                if file_path.name not in processed_files:
                    new_files.append(str(file_path))
                    processed_files.add(file_path.name)
            
            # Process new files
            if new_files:
                self.import_csv_files(new_files)
                
                # Update the processed files list
                with open(CSV_DIRECTORY / ".processed_files.json", "w") as f:
                    json.dump(list(processed_files), f)
                
            return len(new_files)
        except Exception as e:
            logger.error(f"Error watching CSV directory: {e}")
            return 0

    def chunk_documents(self, documents):
        """Split documents into chunks for embedding"""
        if not documents:
            logger.warning("No documents to chunk")
            return []
            
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=100,
            length_function=len,
        )
        chunks = text_splitter.split_documents(documents)
        logger.info(f"Split {len(documents)} documents into {len(chunks)} chunks")
        return chunks
    
    def add_documents_to_vector_store(self, documents):
        """Add documents to the vector store"""
        # Use a lock to prevent concurrent updates
        with self.update_lock:
            if not documents:
                logger.warning("No documents to add to vector store")
                return
                
            chunks = self.chunk_documents(documents)
            
            if not chunks:
                logger.warning("No chunks to add to vector store")
                return
                
            # Create vector store if it doesn't exist
            if self.vector_store is None:
                logger.info("Creating new FAISS vector store")
                self.vector_store = FAISS.from_documents(
                    chunks,
                    self.embeddings
                )
            else:
                # Add documents to existing vector store
                self.vector_store.add_documents(chunks)
            
            # Save the updated vector store
            self.vector_store.save_local(str(VECTOR_STORE_DIRECTORY))
            
            # Save metadata
            self._save_metadata()
            
            # Create or update retriever and chat chain
            self.retriever = self.vector_store.as_retriever(
                search_type="similarity",
                search_kwargs={"k": 5}
            )
            self._create_chain() 
            
            # Update the last update time
            self.last_update_time = datetime.now()
            self._save_update_time()
            
            logger.info(f"Added {len(chunks)} chunks to vector store. Total documents in metadata: {len(self.documents_metadata)}")
    
    def _create_chain(self):
        """Create the RAG chain for answering questions"""
        # Define the prompt template
        template = """
        You are a helpful and friendly customer support AI assistant. Your task is to provide detailed and engaging 
        responses to customer inquiries based on the provided context from customer feedback data.
        
        Special instructions:
        - If the user's question is a greeting or small talk (such as "hi", "hello", "bye", "thank you", "thanks", "goodbye"), respond ONLY with a short, friendly message. Do NOT use the context below for these cases. Example responses: "Hello! How can I help you?", "You're welcome!", "Bye! Have a great day!".
        - For all other questions, answer based strictly on the provided context.
        - Keep your answers factual, short, and directly related to the question.
        
        Context information is below.
        ---------------------
        {context}
        ---------------------

        Given the context information and not prior knowledge, answer the question: {question}

        Your response should be:
        1. Engaging and conversational in tone
        2. Detailed and informative only if necessary . Always give summarized answers .
        3. Based strictly on the provided context, not on prior knowledge
        4. When appropriate, suggest actionable insights 
        5. When the context doesn't provide a clear answer, acknowledge the limitations
         

        If there isn't enough information from the customer feedback data to provide a full answer,
        explain what information you do have (if any), and suggest what kind of information might help.
        
        Response:
        """
        
        prompt = ChatPromptTemplate.from_template(template)
        
        # Create the RAG chain
        self.chain = (
            {"context": self.retriever, "question": RunnablePassthrough()}
            | prompt
            | self.llm
        )
    
    def has_data(self):
        """Check if the vector store has real data beyond the placeholder"""
        # If there's no vector store, we definitely don't have data
        if self.vector_store is None:
            return False
            
        # If we only have the placeholder or no documents, we don't have real data
        if not self.documents_metadata or len(self.documents_metadata) <= 1:
            only_placeholder = list(self.documents_metadata.keys()) == ["placeholder"] if self.documents_metadata else True
            return not only_placeholder
            
        # Otherwise, we probably have data
        return True
    
    def get_answer(self, question):
        """Get an answer to a question"""
        if not self.chain:
            return {
                "answer": "I need some data to learn from first. Please load data from Snowflake or CSV files.",
                "processing_time": "0.0 seconds",
                "has_data": False
            }
        
        if not self.has_data():
            # Try to auto-initialize with data
            try:
                logger.info("No real data found. Attempting to fetch data from Snowflake...")
                df = self.fetch_snowflake_data()
                if not df.empty:
                    documents = self.process_snowflake_data(df)
                    self.add_documents_to_vector_store(documents)
                    logger.info(f"Added {len(documents)} documents from Snowflake")
                else:
                    return {
                        "answer": "I don't have any customer feedback data yet. Please load data from Snowflake or import CSV files with customer feedback.",
                        "processing_time": "0.0 seconds",
                        "has_data": False
                    }
            except Exception as e:
                logger.error(f"Error auto-fetching data: {e}")
                return {
                    "answer": "I don't have any customer feedback data yet, and I wasn't able to automatically fetch data. Please load data from Snowflake or import CSV files with customer feedback.",
                    "processing_time": "0.0 seconds",
                    "has_data": False,
                    "error": str(e)
                }
        
        try:
            start_time = time.time()
            result = self.chain.invoke(question)
            end_time = time.time()
            
            response = {
                "answer": result.content,
                "processing_time": f"{end_time - start_time:.2f} seconds",
                "has_data": True
            }
            
            return response
        except Exception as e:
            logger.error(f"Error getting answer: {e}")
            return {"error": f"Failed to get answer: {str(e)}", "has_data": self.has_data()}
    
    def update_from_snowflake(self, last_updated=None):
        """Update the vector store with new data from Snowflake"""
        # Use a lock to prevent concurrent updates
        with self.update_lock:
            try:
                # If no specific time provided, use last update time or get all data
                if last_updated is None and self.last_update_time is not None:
                    last_updated = self.last_update_time
                
                logger.info(f"Fetching new data from Snowflake since {last_updated}")
                df = self.fetch_snowflake_data(last_updated)
                
                if df.empty:
                    logger.info("No new data in Snowflake")
                    return {"status": "success", "message": "No new data to update"}
                    
                documents = self.process_snowflake_data(df)
                
                if not documents:
                    logger.warning("No valid documents extracted from Snowflake data")
                    return {"status": "warning", "message": "No valid documents extracted from Snowflake data"}
                    
                self.add_documents_to_vector_store(documents)
                
                # Update the last update time
                self.last_update_time = datetime.now()
                
                # Save the last update time to a file for persistence
                self._save_update_time()
                
                return {
                    "status": "success", 
                    "message": f"Updated vector store with {len(documents)} new documents from Snowflake"
                }
            except Exception as e:
                logger.error(f"Error updating from Snowflake: {e}")
                return {"status": "error", "message": str(e)}
                
    def _save_update_time(self):
        """Save the last update time to a file"""
        if self.last_update_time:
            try:
                with open(METADATA_DIRECTORY / "last_update.json", "w") as f:
                    json.dump({"last_update": self.last_update_time.isoformat()}, f)
            except Exception as e:
                logger.error(f"Error saving update time: {e}")
                
    def _load_update_time(self):
        """Load the last update time from a file"""
        try:
            update_file = METADATA_DIRECTORY / "last_update.json"
            if update_file.exists():
                with open(update_file, "r") as f:
                    data = json.load(f)
                    self.last_update_time = datetime.fromisoformat(data["last_update"])
                    logger.info(f"Loaded last update time: {self.last_update_time}")
        except Exception as e:
            logger.error(f"Error loading update time: {e}")
            self.last_update_time = None
    
    def get_data_summary(self):
        """Get a summary of the available data"""
        num_documents = len(self.documents_metadata) if self.documents_metadata else 0
        
        # Remove placeholder document from count
        if "placeholder" in self.documents_metadata:
            num_documents -= 1
            
        sources = {}
        subreddits = {}
        sentiments = {}
        
        for doc_id, metadata in self.documents_metadata.items():
            if doc_id == "placeholder":
                continue
                
            source = metadata.get("source", "unknown")
            sources[source] = sources.get(source, 0) + 1
            
            if source == "snowflake":
                subreddit = metadata.get("subreddit", "unknown")
                subreddits[subreddit] = subreddits.get(subreddit, 0) + 1
                
                sentiment = metadata.get("sentiment", "unknown")
                sentiments[sentiment] = sentiments.get(sentiment, 0) + 1
                
        summary = {
            "total_documents": num_documents,
            "sources": sources,
            "subreddits": subreddits,
            "sentiments": sentiments,
            "last_updated": self.last_update_time.isoformat() if self.last_update_time else None
        }
        
        return summary


def scheduled_update_task(chatbot, interval_hours=1):
    """Background task to periodically update the vector store with new Snowflake data"""
    last_updated = datetime.now() - timedelta(hours=interval_hours)
    result = chatbot.update_from_snowflake(last_updated)
    logger.info(f"Scheduled update: {result['message']}")


def run_auto_sync(interval_hours=1):
    """Run the chatbot with automatic Snowflake synchronization"""
    chatbot = CustomerInsightsChatbot(auto_init=True)
    
    # Load initial data if vector store is empty
    if not chatbot.has_data():
        logger.info("Initial load from Snowflake...")
        df = chatbot.fetch_snowflake_data()
        if not df.empty:
            documents = chatbot.process_snowflake_data(df)
            chatbot.add_documents_to_vector_store(documents)
            logger.info(f"Initial load: Added {len(documents)} documents from Snowflake")
        else:
            logger.warning("No data found in initial Snowflake load")
            print("No data found in Snowflake. You may want to import CSV files instead.")
    
    print(f"Starting auto-sync mode. Will check for new data every {interval_hours} hour(s)")
    print("Enter 'exit' or 'quit' to stop. Enter 'status' to see data summary.")
    print("Enter your questions anytime.")
    
    # Start background thread for periodic updates
    import threading
    import time
    
    stop_event = threading.Event()
    
    def update_thread():
        while not stop_event.is_set():
            try:
                scheduled_update_task(chatbot, interval_hours)
            except Exception as e:
                logger.error(f"Error in scheduled update: {e}")
                
            # Sleep for the specified interval
            for _ in range(int(interval_hours * 60 * 60)):  # Convert hours to seconds
                if stop_event.is_set():
                    break
                time.sleep(1)
    
    # Start the update thread
    sync_thread = threading.Thread(target=update_thread, daemon=True)
    sync_thread.start()
    
    # Interactive question loop
# chatbot = CustomerInsightsChatbot(auto_init=True)

# print("Customer Insights Chatbot initialized.")
# print("Enter your questions or type 'exit' to quit.")

# try:
#     while True:
#         question = input("\nQuestion: ")
        
#         if question.lower() in ['exit', 'quit']:
#             print("Goodbye!")
#             break
            
#         if not question.strip():
#             continue
            
#         try:
#             response = chatbot.get_answer(question)
#             print(f"\n{response['answer']}")
#         except Exception as e:
#             print(f"Error: {str(e)}")
            
# except KeyboardInterrupt:
#     print("\nInterrupted. Exiting...")
#     print("Goodbye!")
if __name__ == "__main__":
    # Any code you want to run only when executing main.py directly
    pass