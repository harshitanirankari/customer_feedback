import os
import uuid
import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

import google.generativeai as genai
from langchain.vectorstores import Chroma
from langchain.schema import Document
from langchain_google_genai import GoogleGenerativeAIEmbeddings
import snowflake.connector

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
CHROMA_DB_DIR = "./chroma_db"
CSV_FILES = [r"C:\Users\Samyuktha M Rao\Project\New_customer_feedback\csv_files\Walmart.csv",r"C:\Users\Samyuktha M Rao\Project\New_customer_feedback\csv_files\Amazon.csv"]
LAST_SYNC_FILE = "last_sync_time.txt"

# Initialize FastAPI app
app = FastAPI(title="Customer Feedback Chatbot API")
app.add_middleware(
    CORSMiddleware, 
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Gemini API for embedding and LLM
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
genai.configure(api_key=GEMINI_API_KEY)
embedding_model = GoogleGenerativeAIEmbeddings(model="models/embedding-001", google_api_key=GEMINI_API_KEY)
vectordb = Chroma(persist_directory=CHROMA_DB_DIR, embedding_function=embedding_model)
llm_model = genai.GenerativeModel("gemini-1.5-flash")

# Snowflake config
snowflake_config = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA")
}

# Pydantic models
class ChatRequest(BaseModel):
    query: str
    max_results: int = 5
    temperature: float = 0.5  # Add temperature for LLM

class ChatResponse(BaseModel):
    answer: str
    sources: List[Dict[str, Any]]



PROCESSED_CSV_LOG = "processed_csv.log"

def has_been_processed(file_path):
    if not os.path.exists(PROCESSED_CSV_LOG):
        return False
    with open(PROCESSED_CSV_LOG, "r") as f:
        return file_path in f.read()

def mark_as_processed(file_path):
    with open(PROCESSED_CSV_LOG, "a") as f:
        f.write(file_path + "\n")













def embed_and_store(text: str, metadata: Dict[str, Any]):
    if not text.strip():
        return
    doc = Document(page_content=text, metadata=metadata)
    vectordb.add_documents([doc])
    logger.info("Embedded and stored feedback: %s", text[:50])
#load csv files

def load_csv_to_chroma(file_path: str):
    if has_been_processed(file_path):
        logger.info("Skipping already processed file: %s", file_path)
        return

    # List of encodings to try
    encodings = ['utf-8', 'latin1', 'ISO-8859-1', 'cp1252']
    
    for encoding in encodings:
        try:
            logger.info(f"Trying to load {file_path} with {encoding} encoding")
            df = pd.read_csv(file_path, encoding=encoding)
            
            # Find feedback column
            feedback_column = next((col for col in df.columns if col.lower() in ["feedback", "comment", "review", "text"]), None)
            if not feedback_column:
                logger.warning(f"No feedback column found in {file_path}")
                raise ValueError("No feedback column found")
            
            # Process and store each row
            for _, row in df.iterrows():
                feedback = str(row[feedback_column])
                metadata = {k: str(v) for k, v in row.items() if k != feedback_column}
                
                # Improved source detection
                if "Flipkart" in file_path:
                    source_name = "Flipkart"
                elif "Amazon" in file_path:
                    source_name = "Amazon"
                elif "Walmart" in file_path:
                    source_name = "Walmart"
                else:
                    source_name = "Unknown"
                
                metadata.update({
                    "source": "csv",
                    "website": source_name,
                    "filename": os.path.basename(file_path),
                    "timestamp": datetime.now().isoformat()
                })
                embed_and_store(feedback, metadata)
                
            logger.info(f"Successfully loaded and embedded file: {file_path} with {encoding} encoding")
            mark_as_processed(file_path)
            return  # Successfully processed, exit the function
            
        except UnicodeDecodeError:
            # Try the next encoding
            continue
        except Exception as e:
            logger.error(f"Error loading CSV {file_path}: {e}")
            raise
            
    # If we get here, none of the encodings worked
    logger.error(f"Failed to load {file_path} with any of the tried encodings: {encodings}")

# Sync Snowflake data into ChromaDB
def sync_snowflake():
    try:
        conn = snowflake.connector.connect(**snowflake_config)
        cursor = conn.cursor()

        last_sync_time = None
        if os.path.exists(LAST_SYNC_FILE):
            with open(LAST_SYNC_FILE, "r") as f:
                last_sync_time = f.read().strip()

        query = f"""
            SELECT * FROM reddit_feedback_final
            {f"WHERE INGESTION_TIME > '{last_sync_time}'" if last_sync_time else ""}
            ORDER BY INGESTION_TIME DESC
            LIMIT 100
        """
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]

        count = 0
        latest_time = last_sync_time
        for row in cursor:
            row_data = dict(zip(columns, row))
            feedback = str(row_data.get("TEXT", ""))
            if not feedback.strip():
                continue
            metadata = {k.lower(): str(v) for k, v in row_data.items() if k != "TEXT"}
            metadata["source"] = "snowflake"
            embed_and_store(feedback, metadata)
            count += 1
            ingestion_time = row_data.get("INGESTION_TIME")
            if ingestion_time:
                str_time = str(ingestion_time)
                if latest_time is None or str_time > latest_time:
                    latest_time = str_time

        if latest_time:
            with open(LAST_SYNC_FILE, "w") as f:
                f.write(latest_time)

        cursor.close()
        conn.close()
        logger.info("Synced %d records from Snowflake.", count)
        return {"status": "success", "count": count, "latest_timestamp": latest_time}

    except Exception as e:
        logger.error("Error syncing from Snowflake: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

# Retrieve top-k documents and generate LLM answer
def generate_answer(query: str, k: int, temperature: float = 0.2):
    # Small talk responses
    small_talk = {
        "hi": "Hello! How can I help you today?",
        "hello": "Hi there! How can I assist you?",
        "hey": "Hey! What feedback would you like to know about?",
        "bye": "Goodbye! Have a great day!",
        "thank you": "You're welcome!",
        "thanks": "Glad I could help!",
        "goodbye": "See you soon!",
    }
    q_lower = query.strip().lower()
    for key in small_talk:
        if key in q_lower and len(q_lower) <= len(key) + 5:
            return {"answer": small_talk[key], "sources": []}

    results = vectordb.similarity_search_with_score(query, k=k)
    if not results:
        # Fallback to small talk if detected
        for key in small_talk:
            if key in q_lower:
                return {"answer": small_talk[key], "sources": []}
        return {"answer": "Sorry, no relevant feedback found.", "sources": []}

    context = "\n\n".join([
        f"Feedback {i+1} from {doc.metadata.get('website', 'Unknown')}: {doc.page_content}"
        for i, (doc, _) in enumerate(results)
    ])

    prompt = f"""
You are a helpful assistant analyzing customer feedback.
Use only the information provided below to answer the question.Respond to the basic greetings . 
If you cant answer then respond with "Sorry, I dont have the information to answer this question. After fetching few real-time comments I might be able to ."

USER QUESTION: {query}

FEEDBACK DATA:
{context}

Give a short, specific, and helpful response.
"""

    response = llm_model.generate_content(prompt, generation_config={"temperature": temperature})
    sources = [{"text": doc.page_content[:150], "metadata": doc.metadata} for doc, _ in results]
    return {"answer": response.text.strip(), "sources": sources}

# FastAPI Endpoints
@app.post("/chat", response_model=ChatResponse)
def chat(request: ChatRequest):
    try:
        response = generate_answer(request.query, request.max_results, getattr(request, 'temperature', 0.2))
        return ChatResponse(**response)
    except Exception as e:
        logger.error("Chat error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sync/snowflake")
def sync_now():
    return sync_snowflake()

@app.on_event("startup")
def on_startup():
    logger.info("Loading local CSVs into ChromaDB...")
    for path in CSV_FILES:
        if os.path.exists(path):
            load_csv_to_chroma(path)
        else:
            logger.warning("CSV file not found: %s", path)
    logger.info("Performing initial sync from Snowflake...")
    sync_snowflake()
    logger.info("Startup complete.")

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now().isoformat()}

# Uvicorn entry point
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
