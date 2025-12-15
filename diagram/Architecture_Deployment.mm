flowchart TB

%% =========================
%% SOURCE SYSTEMS
%% =========================
subgraph SOURCE[Source Systems]
SRC[External Organisations]
end

SRC -->|Secure Data Exchange| LAND

%% =========================
%% LANDING ZONE
%% =========================
subgraph LANDING[Landing Zone]
LAND[File Server Raw Files]
end

LAND --> BRONZE

%% =========================
%% LAKEHOUSE ARCHITECTURE
%% =========================
subgraph LAKEHOUSE[Lakehouse Architecture]
BRONZE[Bronze Raw Data]
SILVER[Silver Cleaned Data]
DIMS[Gold Dimensions]
DIMDATE[Dim Date]
FACT[Gold Fact Table]
end

BRONZE --> SILVER
SILVER --> DIMS
SILVER --> DIMDATE
DIMS --> FACT
DIMDATE --> FACT

%% =========================
%% CONSUMPTION
%% =========================
subgraph CONSUMPTION[Consumption Layer]
BI[Power BI Analytics]
end

FACT --> BI

%% =========================
%% ORCHESTRATION
%% =========================
subgraph ORCH[Orchestration]
JOB[Azure Databricks Workflow]
end

JOB --> BRONZE
JOB --> SILVER
JOB --> DIMS
JOB --> DIMDATE
JOB --> FACT

%% =========================
%% DATA GOVERNANCE
%% =========================
subgraph GOV[Data Governance and Quality]
DQ[Data Quality Controls]
META[Metadata and Lineage]
end

SILVER --> DQ
FACT --> META

%% =========================
%% PLATFORM ENGINEERING
%% =========================
subgraph PLATFORM[CI CD Infrastructure and Automation]
GIT[GitHub Repository]
DEPLOY[Databricks Repos and Jobs Deployment]
TERRAFORM[Terraform Infrastructure]
end

GIT --> DEPLOY --> JOB
TERRAFORM --> JOB
