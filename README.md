# OT Data Modernisation – Lakehouse Proof of Concept

## Overview

This repository contains a **Lakehouse Proof of Concept (PoC)** built on **Azure Databricks**, designed to demonstrate how OT can modernise data ingestion, transformation, governance, and analytics using a **Bronze / Silver / Gold** architecture.

The PoC reflects OT’s **current programme phase**, which focuses on **Analysis & Design (A&D)** rather than full-scale development.  
The goal is to validate architecture, data standards, governance boundaries, and migration patterns ahead of any SAS-to-Databricks implementation.

---

## Objectives

- Demonstrate a **modern lakehouse architecture** aligned with OT standards
- Apply **data quality and governance principles** early in the pipeline
- Separate **raw ingestion**, **business rule enforcement**, and **analytics-ready models**
- Provide a reference design for:
  - SAS modernisation analysis
  - Data sharing ingestion (e.g. SFTP, secure exchanges)
  - Future CI/CD and Infrastructure as Code (IaC)

---

## Architecture Summary

### High-level Flow
