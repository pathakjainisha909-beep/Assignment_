

## Project Overview

This repository contains three main components:

1. **abstraction_layer**: The core backend API built with FastAPI, featuring NL2SQL functionality using the Gemini API, a local frontend for interaction, and support for future embeddings (currently disabled).
2. **api_integration**: Scripts to fetch data from external sources (e.g., Bigin, Rolodex) to populate the database.
3. **schema_design**: Data modeling scripts and raw CSV files for defining and migrating the database schema.

### Purpose
- Enable querying a PostgreSQL database using natural language via a web interface.
- Integrate external data sources for a unified dataset.
- Provide a structured schema for scalable data management.

## Prerequisites
- **Python 3.8+**
- **PostgreSQL 15+** 
- **Node.js** (for frontend)
- **GitHub Account** and GitHub Desktop (for managing the repository)

## Installation

### Backend Setup
1. **Clone the Repository**:
  git clone https://github.com/pathakjainisha909-beep/Task_Assignment.git

# schema_design

add a Raw Data folder in schema_design folder from the drive link
drive link : [https://drive.google.com/drive/folders/1dLZFCiwm2O8MndEwVpHJxIeG6JMliDUU?usp=drive_link](https://drive.google.com/drive/folders/1dLZFCiwm2O8MndEwVpHJxIeG6JMliDUU?usp=sharing)

# /abstraction_layer

# Frontend

npm install

npm run dev

# Backend

2. **Set Up Virtual Environment**:
 python -m venv venv
venv\Scripts\activate

pip install -r requirements.txt





