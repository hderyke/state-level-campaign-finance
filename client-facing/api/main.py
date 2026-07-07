from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from db import build_summary
from routers import candidates, committees, contributions, downloads, expenditures


@asynccontextmanager
async def lifespan(app: FastAPI):
    build_summary()
    yield


app = FastAPI(
    title="State Campaign Finance API",
    description="Search campaign finance records across U.S. states.",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(candidates.router,     prefix="/candidates",    tags=["Candidates"])
app.include_router(committees.router,     prefix="/committees",    tags=["Committees"])
app.include_router(contributions.router,  prefix="/contributions", tags=["Contributions"])
app.include_router(downloads.router,      prefix="/downloads",     tags=["Downloads"])
app.include_router(expenditures.router,   prefix="/expenditures",  tags=["Expenditures"])


@app.get("/health", tags=["Meta"])
def health():
    return {"status": "ok"}
