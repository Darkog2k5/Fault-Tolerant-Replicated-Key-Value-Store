import os
import uvicorn
from fastapi import FastAPI

PORT = int(os.getenv("PORT", 8080))
app = FastAPI(title="Client Proxy Stub")

@app.get("/api/v1/health")
async def health():
    return {"status": "ALIVE", "role": "PROXY"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)