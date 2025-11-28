from __future__ import annotations
from typing import Annotated, Dict, List, Optional, Sequence, Literal, Union, Any
from pydantic import BaseModel, Field


class MappingConfigModel(BaseModel):
    reference_fields: Sequence[str] = Field(default_factory=list)
    synonyms: Optional[Dict[str, List[str]]] = None
    threshold: Annotated[float, Field(ge=0.0, le=1.0)] = 0.7
    epsilon: Annotated[float, Field(ge=0.0, le=1.0)] = 0.05

class DatasetReport(BaseModel):
    name: str
    path: Optional[str]
    rows: int
    cols: int
    schema: Dict[str, str]
    semantic_types: Dict[str, str]
    statistics: Dict[str, Any]
    nested_structures: List[str]
    categorical_cols: List[str]
    llm_insights: Dict[str, Any]
    mapping: Dict
    ambiguous: List[str]
    unmapped: List[str]
    anomalies: Dict[str, int]
    anomaly_samples_saved: Dict[str, Optional[str]]


class AnomalyConfigModel(BaseModel):
    z_threshold: float = 3.0
    use_iqr: bool = True
    use_zscore: bool = True
    use_isolation_forest: bool = True
    contamination: Annotated[float, Field(gt=0.0, lt=0.5)] = 0.01
    n_estimators: int = 100
    random_state: int = 42


class LocalSourceModel(BaseModel):
    type: Literal["local"] = "local"
    root: str
    max_rows: Optional[int] = None


class SQLSourceModel(BaseModel):
    type: Literal["sql"] = "sql"
    query: Optional[str] = None
    schema: Optional[str] = None
    max_rows: Optional[int] = None


class RunRequest(BaseModel):
    source: Annotated[Union[LocalSourceModel, SQLSourceModel], Field(discriminator="type")]
    mapping: MappingConfigModel
    anomaly: Optional[AnomalyConfigModel] = None


class RunAccepted(BaseModel):
    report_id: str
    status: str


class ArtifactItem(BaseModel):
    name: str
    size: int
    dataset_name: Optional[str] = None
    method: Optional[str] = None


class ArtifactList(BaseModel):
    items: Annotated[List[ArtifactItem], Field(min_length=0)] = Field(default_factory=list)
