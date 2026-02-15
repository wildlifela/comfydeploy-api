import os
import re
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import AsyncAdaptedQueuePool
from dotenv import load_dotenv
from contextlib import asynccontextmanager

load_dotenv()

# Use environment variables for database connection
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    # Handle postgres:// scheme (Neon) → postgresql+asyncpg://
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
    elif not DATABASE_URL.startswith("postgresql+asyncpg://"):
        DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

    # Strip query params incompatible with asyncpg (sslmode, channel_binding)
    DATABASE_URL = re.sub(r"[?&](sslmode|channel_binding)=[^&]*", "", DATABASE_URL)
    # Clean up leftover ? or & at the end
    DATABASE_URL = re.sub(r"[?&]$", "", DATABASE_URL)

connect_args = {}
if DATABASE_URL and "neon" in DATABASE_URL:
    connect_args["ssl"] = "require"

MAX_EXPECTED_CONCURRENCY = 200  # Document your design target

# Configure engine with larger pool size and longer timeout
engine = create_async_engine(
    DATABASE_URL,
    connect_args=connect_args,
    poolclass=AsyncAdaptedQueuePool,
    # Neon recommended settings for serverless
    pool_size=25,  # Reduced from 100 - better for 2 workers on 4 CPUs
    max_overflow=50,  # Reduced from 200 - still allows for bursts
    pool_timeout=30,  # Shorter timeout as Neon quickly provisions connections
    pool_pre_ping=True,  # Keep enabled to verify connection health
    pool_recycle=1800,  # 30 minutes recycle to align with Neon's timeout
    pool_use_lifo=True,  # Last In First Out - better for serverless
)

AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session

@asynccontextmanager
async def get_db_context():
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()