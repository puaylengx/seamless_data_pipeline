from __future__ import annotations
import os
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class PipelineConfig:
    package_root: Path
    query_path: Path
    bq_project: str
    bq_dataset: str
    bq_table: str
    bq_raw_table: str
    bq_location: str
    bq_key_path: Path | None
    sky_username: str
    sky_password: str
    sky_host: str
    sky_database: str
    min_academic_year: int
    allowed_statuses: tuple[str, ...]
    
    @classmethod
    def from_env(cls, package_root: Path | None = None) -> "PipelineConfig":
        if package_root is None:
            package_root = Path(__file__).resolve().parent
            
        def require(name: str) -> str:
            v = os.getenv(name)
            if not v:
                raise RuntimeError(f"Mission environment variable {name}")
            return v
        
        query_path = package_root / "sql" / "student_information.sql"
        if not query_path.exists():
            raise FileNotFoundError(query_path)
        
        # ไม่บังคับ GOOGLE_APPLICATION_CREDENTIALS เพื่อรองรับ ADC
        key_env = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        key_path = None
        if key_env:
            p = Path(key_env)
            key_path = p if p.is_absolute() else package_root.parents[3] / p
            if not key_path.exists:
                raise FileNotFoundError(key_path)
            
        statuses = tuple(s.strip() for s in os.getenv(
            "SKY_STATUSES", "dm,ex,g,la,np,prc,pa,rs,s"
        ).split(",") if s.strip())
        
        return cls(
            package_root=package_root,
            query_path=query_path,
            bq_project=os.getenv("BQ_PROJECT", "your_gcp_project"),
            bq_dataset=os.getenv("BQ_DATASET", "Education"),
            bq_table=os.getenv("BQ_RAW_TABLE", "student_info"),
            bq_location=os.getenv("BQ_LOCATION", "asia-southeast1"),
            bq_key_path=key_path, # อาจเป็นค่า None
            sky_username=require("SKY_USERNAME"),
            sky_password=require("SKY_PASSWORD"),
            sky_host=require("SKY_HOST"),
            sky_database=require("SKY_DATABASE"),
            min_academic_year=int(os.getenv("MIN_ACADEMIC_YEAR", "2016")),
            allowed_statuses=statuses
        )
        