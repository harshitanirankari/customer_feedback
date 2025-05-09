from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
import uvicorn
import pathlib
from chatbot import CustomerInsightsChatbot

# Create directories
pathlib.Path("./uploaded_csvs").mkdir(exist_ok=True)

# Initialize bot
chatbot = CustomerInsightsChatbot(auto_init=True)

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------- Schemas -----------
class QuestionRequest(BaseModel):
    question: str

class AnswerResponse(BaseModel):
    answer: str

# ----------- Endpoints -----------
@app.post("/ingest/snowflake")
def ingest_snowflake():
    try:
        result = chatbot.update_from_snowflake()
        return {"message": result["message"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ingest/csv")
async def ingest_csv(files: List[UploadFile] = File(...)):
    try:
        file_paths = []
        for file in files:
            path = f"./uploaded_csvs/{file.filename}"
            with open(path, "wb") as f:
                f.write(await file.read())
            file_paths.append(path)
        
        result = chatbot.import_csv_files(file_paths)
        return {"message": result["message"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/chat", response_model=AnswerResponse)
def chat(request: QuestionRequest):
    try:
        result = chatbot.get_answer(request.question)
        return AnswerResponse(answer=result['answer'])
    except Exception as e:
        return AnswerResponse(answer=f"Error processing request: {str(e)}")

@app.get("/status")
def status():
    try:
        return {
            "metadata_entries": len(chatbot.documents_metadata),
            "documents_indexed": chatbot.vector_store.index.ntotal if chatbot.vector_store else 0,
            "data_summary": chatbot.get_data_summary()
        }
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)