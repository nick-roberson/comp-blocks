from pydantic import BaseModel


class BlockParamBase(BaseModel):
    """Base class for all block parameters."""

    # Log level for the block
    log_level: str = "INFO"
    # Try to run the block only once
    attempts: int = 1
    # 1 second delay between retries
    retry_delay: int = 1
