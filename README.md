# Text Processing & Embedding Pipeline

This assignment processes PDF and DOCX files and prepares them for semantic search by generating embeddings and storing them in a PostgreSQL database.

The pipeline includes:

- Text extraction from PDF / DOCX
- Fixing Hebrew RTL issues (reversed words from PDF)
- Text cleaning
- Chunking strategy based on file type
- Embedding generation using Gemini
- Storage of chunks and embeddings in PostgreSQL

---

# 🔐 Security

Sensitive data is not stored in the code.

Instead, the project uses a `.env` file that contains:

- `GEMINI_API_KEY`
- `POSTGRES_URL`

This ensures API keys and database credentials remain secure.

---

# ⚙️ Installation Guide

Before running the project, make sure you are using an up-to-date version of Python.

It is recommended to use **Python 3.10 or higher**.

**PostgreSQL (with pgAdmin)** must be installed locally.

---

## Step 1 — Check Python Version

### Windows

`python --version` 

### macOS

`python3 --version` 

---

## Step 2 — Create Virtual Environment

### Windows

`python -m venv .venv`  
`.venv\Scripts\activate`

### macOS

`python3 -m venv .venv`  
`source .venv/bin/activate`

---

## Step 3 — Upgrade pip

### Windows

`python.exe -m pip install --upgrade pip`

### macOS

`python3 -m pip install --upgrade pip`

---

## Step 4 — Install Required Libraries

Install all required imports:

`pip install google-genai`  
`pip install python-dotenv`  
`pip install psycopg2-binary`  
`pip install pypdf`  
`pip install python-docx`

Or install everything together:

`pip install google-genai python-dotenv psycopg2-binary pypdf python-docx`

---

# 🔑 Environment Variables Setup

Create a `.env` file in the project root:

`GEMINI_API_KEY=your_api_key_here`  
`POSTGRES_URL=your_connection_string_here`

---
# ▶️ Running the Pipeline

Before running the script, insert the file path you want to process inside the code.

Locate the following section in `index_documents.py`:


#### ------------------------------- Insert your path file here ------------------------------->
file_path = r"C:\path\to\your\document.docx" #TODO
#### ------------------------------------------------------------------------------------------>

---

After updating the path, run the script:

### Windows  
`python index_documents.py`

### macOS  
`python3 index_documents.py`

---

# 📌 Usage Examples

Below are examples of how to use the pipeline with different file types.

---

## Example 1 — Process a PDF File (Windows)

Update the file path inside `index_documents.py`:

`file_path = r"C:\Users\YourName\Documents\report.pdf"`

Run:

`python index_documents.py`

---

## Example 2 — Process a DOCX File (Windows)

`file_path = r"C:\Users\YourName\Documents\summary.docx"`

Run:
 
`python index_documents.py`


---

## Example 3 — macOS Path

`file_path = "/Users/YourName/Documents/report.pdf"`

Run:

`python3 index_documents.py`

---

# 🗄 PostgreSQL Usage

PostgreSQL is used to store the processed output of the pipeline.

Each stored record includes:

- `id` - unique identifier  
- `chunk_text` - text segment  
- `embedding` - embedding vector  
- `filename` - original file name  
- `split_strategy` - selected chunking method  
- `created_at` - insertion timestamp (optional)

Connection is handled via:

`POSTGRES_URL` in the `.env` file

Stored data can be viewed in pgAdmin under:
Databases → your_database → Schemas → public → Tables → indexed_documents
