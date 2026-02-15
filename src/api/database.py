import os
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
    # Ensure the URL uses the asyncpg dialect
    if not DATABASE_URL.startswith("postgresql+asyncpg://"):
        DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

    # asyncpg does not support sslmode or channel_binding as URL params
    # (Neon adds these by default). Strip them and let asyncpg handle SSL natively.
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
    _parsed = urlparse(DATABASE_URL)
    _params = parse_qs(_parsed.query)
    _params.pop("sslmode", None)
    _params.pop("channel_binding", None)
    DATABASE_URL = urlunparse(_parsed._replace(query=urlencode(_params, doseq=True)))

MAX_EXPECTED_CONCURRENCY = 200  # Document your design target

# Configure engine with larger pool size and longer timeout
engine = create_async_engine(
    DATABASE_URL,
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