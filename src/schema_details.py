"""
Module to provide metadata details for the database schema.

This module defines a function, `Metadata`, that returns a list of strings
describing the database schema.  This metadata is used by the NL-to-SQL
conversion process to generate valid SQL queries.
"""

import logging
from fastapi import HTTPException

# Configure logging (if not already configured)
if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def Metadata() -> list | None:

    """
    Provides metadata about the database schema.

    Returns:
        List[str]: A list of strings, where each string describes a table
                   and its columns. Returns an empty list if no schema
                   information is available.
    """
    try:
        return [
            "The database tables to query are student, exam, ACCESS_RING_DATA and placement:"
            
            "student table: roll_number INT PRIMARY KEY, sname VARCHAR(30), dept VARCHAR(5), sem INT",
            "exam table: regno INT PRIMARY KEY, roll_number INT Foreign key, dept VARCHAR(5), mark1 int,mark2,mark3 int,mark4 int,mark5 int,total int,average int,grade varchar(3)",
            "placement table: placementID INT PRIMARY KEY, roll_number INT, dept char(5), company VARCHAR(100),salary int"
        ]
    except Exception as e:
        logger.error(f"Error getting metadata: {e}")
        raise HTTPException(status_code=500,detail="There is no schema details")