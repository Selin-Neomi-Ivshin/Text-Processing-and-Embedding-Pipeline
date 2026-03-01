import os
import re
from pathlib import Path
from dotenv import load_dotenv
from docx import Document
from pypdf import PdfReader
import logging
from google import genai
import psycopg2
from psycopg2.extras import execute_values

# Suppress verbose logging from pypdf to show only errors
logging.getLogger("pypdf").setLevel(logging.ERROR)

# Regex pattern to detect sequences of at least two Hebrew letters
HEBREW_RE = re.compile(r"[א-ת]{2,}")

def fix_reversed_hebrew_words(text):
    '''
    Fixes reversed Hebrew words caused by PDF extraction issues.

    During PDF parsing, Hebrew text is sometimes extracted in reverse order.
    This function detects continuous Hebrew letter sequences and reverses them
    to restore correct word orientation.

    Parameters:
        text (str): Extracted raw text from PDF.

    Returns:
        str: Text with corrected Hebrew word direction.
    '''

    # Reverse matched Hebrew word
    def _rev(match):
        return match.group(0)[::-1]

    # Apply reversal on each Hebrew sequence
    return HEBREW_RE.sub(_rev, text)

def extract_text_from_pdf(path):
    '''
    Extracts clean text from a PDF file.

    The function:
    - Validates the file type
    - Reads text page by page
    - Fixes reversed Hebrew words
    - Normalizes excessive whitespace

    Parameters:
        path (str): File path to the PDF.

    Returns:
        str: Cleaned text extracted from the PDF.
             Returns an error message if the file is not a PDF.
    '''

    reader = PdfReader(path)
    parts: list[str] = []

    # Iterate through all pages
    for page in reader.pages:
        # Extract and clean raw text
        text = (page.extract_text() or "").strip()

        # Skip empty pages
        if not text:
            continue

        # Fix reversed Hebrew text caused by PDF parsing
        text = fix_reversed_hebrew_words(text)
        # Normalize multiple spaces/tabs into single space
        text = re.sub(r"[ \t]{2,}", " ", text)
        parts.append(text)
    # Join all cleaned pages into one text block
    return "\n".join(parts)


def extract_text_from_docx(path):
    '''
    Extracts text from a DOCX or DOC file.

    The function:
    - Validates the file type
    - Reads all paragraphs from the document
    - Collects non-empty text content

    Parameters:
        path (str): File path to the DOCX/DOC file.

    Returns:
        str: Extracted text joined by newline characters.
             Returns an error message if the file is not a DOCX/DOC file.
    '''

    doc = Document(path)
    parts = []

    # Iterate through document paragraphs
    for p in doc.paragraphs:
        # Append only non-empty text
        if p.text:
            parts.append(p.text)
    # Join all paragraphs into one text block
    return "\n".join(parts)


def clean_text(text, is_pdf):
    '''
    Cleans and normalizes extracted text from documents.

    The function performs:
    - Newline normalization
    - Fixing broken words caused by PDF line wrapping
    - Removal of extraction artifacts
    - Bullet normalization
    - Space cleanup
    - Optional PDF-specific structural fixes

    Parameters:
        text (str): Raw extracted text.
        is_pdf (bool): Indicates whether the source is a PDF.
                       Enables additional PDF-specific cleaning.

    Returns:
        str: Cleaned and normalized text ready for downstream processing.
    '''
    # Return empty string if input is None or empty
    if not text:
        return ""

    # Normalize newlines
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # Fix hyphenated words split across lines (common in PDFs)
    # Example: "some-\nthing" -> "something"
    text = re.sub(r"([A-Za-zא-ת])-\n([A-Za-zא-ת])", r"\1\2", text)

    # Remove transcript-style markers like <<text>>
    text = re.sub(r"<<.*?>>", "", text)

    # Unescape quotes
    text = text.replace("\\'", "'").replace('\\"', '"')

    # Replace bullet symbols with newline
    text = text.replace("•", "\n")
    text = text.replace("§", "\n")

    # --- PDF-SPECIFIC CLEANING ---
    if is_pdf:
        # Convert inline bullets like ": o something" into newline
        text = re.sub(r"(?<=:)\s*o\s+", "\n", text)

        # Fix bracket artifacts: "Chunks)" -> "(Chunks)"
        text = re.sub(r"\b([A-Za-z]{2,})\)", r"(\1)", text)

        # --- NEW: Treat single newlines as line wraps, keep paragraph breaks ---
        # Normalize excessive blank lines into paragraph breaks
        text = re.sub(r"\n{3,}", "\n\n", text)

        # Convert single newline inside a paragraph into a space
        # while preserving paragraph breaks
        text = re.sub(r"(?<!\n)\n(?!\n)", " ", text)

        # 3) Remove extra spaces created during joining
        text = re.sub(r"[ ]{2,}", " ", text)

        # 4) Re-normalize paragraph spacing
        text = re.sub(r"\n{3,}", "\n\n", text)

        # Remove PDF glyph-id artifacts like "/gid00030/gid00035/..."
        text = re.sub(r"(?:\s*/gid\d+)+", " ", text)

    # Replace tabs with spaces
    text = text.replace("\t", " ")

    # Collapse multiple spaces
    text = re.sub(r"[ ]{2,}", " ", text)

    # Trim spaces around newlines
    text = re.sub(r"[ \u00A0]*\n[ \u00A0]*", "\n", text)

    # Remove lines made only of separators like "---"
    text = re.sub(r"(?m)^\s*[-–—]{2,}\s*$\n?", "", text)

    # Collapse blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text.strip()


def extract_and_clean_text(file_path):
    '''
    Extracts and cleans text from supported document types.

    The function:
    - Validates file existence
    - Detects file type
    - Extracts raw text using the appropriate method
    - Applies standardized cleaning

    Supported formats:
        - PDF
        - DOCX
        - DOC (handled similarly to DOCX)

    Parameters:
        file_path (str): Path to the input file.

    Returns:
        tuple:
            cleaned_text (str): Normalized and cleaned text.
            filename (str): Name of the processed file.
    '''
    path = Path(file_path)
    # Validate file existence
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    suffix = path.suffix.lower()

    # Handle PDF files
    if suffix == ".pdf":
        raw = extract_text_from_pdf(str(path)) # Extract raw text from PDF
        cleaned = clean_text(raw, is_pdf=True) # Apply PDF-specific cleaning
        return cleaned, path.name

    # Handle DOCX and DOC files
    if suffix == ".docx" or suffix == ".doc":
        raw = extract_text_from_docx(str(path)) # Extract raw text from Word document
        cleaned = clean_text(raw, is_pdf=False) # Apply standard cleaning (non-PDF)
        return cleaned, path.name

    raise ValueError("Unsupported file type. Please provide a .pdf or .docx file.")


def chunk_pdf_fixed(text, chunk_size = 1000, overlap = 150):
    '''
    Splits long text into fixed-size chunks with overlap.

    The function:
    - Creates chunks close to the desired size
    - Avoids breaking words at chunk boundaries
    - Ensures overlapping context between chunks
    - Prevents chunks from starting mid-word

    Parameters:
        text (str): Input text to split.
        chunk_size (int): Target size of each chunk.
        overlap (int): Number of overlapping characters between chunks.

    Returns:
        list[str]: List of text chunks.
    '''

    # Normalize whitespace
    text = re.sub(r"\s+", " ", (text or "")).strip()
    if not text:
        return []

    chunks = []
    start = 0
    n = len(text)

    while start < n:
        end = min(start + chunk_size, n)

        # Avoid ending mid-word: cut at nearest space up to 50 chars back
        if end < n:
            cut = text.rfind(" ", start, end)
            if cut != -1 and (end - cut) <= 50 and cut > start:
                end = cut

        chunk = text[start:end].strip()

        # Add valid chunk
        if chunk:
            chunks.append(chunk)

        if end == n:
            break

        # Create overlap but avoid starting mid-word
        start = max(0, end - overlap)
        if start > 0 and text[start] != " " and text[start - 1] != " ":
            next_space = text.find(" ", start, min(start + 50, n))
            if next_space != -1:
                start = next_space + 1  # Move start after space

    return chunks


def split_long_paragraph(paragraph, max_chars = 1000):
    '''
    Splits a single long paragraph into sub-chunks using sentence boundaries only.

    Guarantees:
    - No word is cut in the middle
    - No sentence is cut in the middle

    Parameters:
        paragraph (str): One paragraph string (no newlines).
        max_chars (int): Preferred maximum size for each sub-chunk.

    Returns:
        list[str]: A list of sentence-based sub-chunks.
    '''
    p = re.sub(r"\s+", " ", (paragraph or "")).strip()
    if not p:
        return []

    # Split paragraph into sentences based on punctuation endings (. ! ?)
    sentences = re.split(r"(?<=[.!?])\s+", p)

    sub_chunks = []
    current = ""

    for s in sentences:
        s = s.strip()
        if not s:
            continue

        # Append sentence if it fits the current sub-chunk; otherwise start a new one
        if not current or len(current) + 1 + len(s) <= max_chars:
            current = (current + " " + s).strip()
        else:
            sub_chunks.append(current)
            current = s

    if current:
        sub_chunks.append(current)

    return sub_chunks


def chunk_docx_paragraphs(text, min_chunk_chars = 250, max_paragraph_chars = 1500, tolerance = 300):
    '''
    Splits DOCX text into semantic chunks using a paragraph-first strategy.

    Behavior:
    - Buffers paragraphs together until reaching min_chunk_chars
    - Treats short headings as part of the following content (avoids flushing on headings)
    - Keeps bullets grouped (bullets stay in the same buffer)
    - Avoids creating chunks that are "label-like" (e.g., "Amount:" / "סוג התביעה:")
      by not flushing immediately after such lines
    - Splits only very large paragraphs into sentence-based sub-chunks
      (only when paragraph length exceeds max_paragraph_chars + tolerance)

    Parameters:
        text (str): Input text where paragraphs are separated by newlines.
        min_chunk_chars (int): Minimum chunk size before flushing the buffer.
        max_paragraph_chars (int): Preferred maximum size for a single paragraph (sub-chunk max).
        tolerance (int): Extra allowed length before splitting a paragraph.

    Returns:
        list[str]: Final list of semantic chunks.
    '''

    def is_bullet(p):
        # Detect common bullet starters
        return p.startswith(("-", "•", "*"))

    def is_heading(p):
        # Heuristic: short line, not ending with '.', and not a bullet
        return (len(p) <= 70) and (not p.endswith(".")) and (not is_bullet(p))

    def is_label_like(p):
        # Heuristic: short "field label" line ending with ":" (common across many document types)
        return len(p) <= 80 and p.endswith(":")

    def normalize_paragraph(p):
        # Normalize bullet markers to a consistent "- " style
        if p.startswith(("•", "*")):
            return "- " + p[1:].lstrip()
        return p

    def flush_buffer(buf, chunks):
        # Convert buffered paragraphs into a single cleaned chunk
        chunk = re.sub(r"\s+", " ", " ".join(buf)).strip()
        if chunk:
            chunks.append(chunk)

    # Split text into clean paragraph list
    paras = [normalize_paragraph(p.strip()) for p in (text or "").split("\n") if p.strip()]

    chunks = []
    buf = []
    buf_len = 0  # Track buffer length to avoid repeated sum(len(...))

    for p in paras:
        # If a paragraph is significantly too large, split it by sentences
        if len(p) > (max_paragraph_chars + tolerance):
            # Flush any buffered content so we don't mix unrelated paragraphs
            if buf:
                flush_buffer(buf, chunks)
                buf, buf_len = [], 0

            # Split this one paragraph into sentence-based sub-chunks
            chunks.extend(split_long_paragraph(p, max_chars=max_paragraph_chars))
            continue

        # Otherwise, keep paragraph in buffer (paragraph-first)
        buf.append(p)
        buf_len += len(p)

        # Flush when we have enough content AND the last line isn't a heading or a label-like field
        # This helps headings/labels attach naturally to the content that follows.
        if (not is_heading(p)) and (not is_label_like(p)) and buf_len >= min_chunk_chars:
            flush_buffer(buf, chunks)
            buf, buf_len = [], 0

    # Flush any remaining buffered text
    if buf:
        flush_buffer(buf, chunks)

    return chunks

def chunk_by_file_type(file_path, cleaned_text):
    '''
    Splits cleaned text into chunks based on the original file type.

    The function determines the chunking strategy:
    - PDFs → fixed-size chunking with overlap
    - DOCX / DOC → paragraph-based semantic chunking

    This also returns the split strategy so it can be stored
    for traceability in downstream processes (e.g. database).

    Parameters:
        file_path (str): Original file path.
        cleaned_text (str): Text after extraction and cleaning.

    Returns:
        tuple:
            chunks (list[str]): Generated text chunks
            split_strategy (str): Strategy used for chunking
    '''

    suffix = Path(file_path).suffix.lower() # Detect file extension

    # Use fixed chunking for PDFs
    if suffix == ".pdf":
        return chunk_pdf_fixed(cleaned_text, chunk_size=1400, overlap=200), "Fixed size with overlap"

    # Use paragraph-based chunking for Word documents
    if suffix in (".docx", ".doc"):
        return chunk_docx_paragraphs(cleaned_text), "Paragraph based splitting"

    raise ValueError("Unsupported file type for chunking.") # Unsupported file type


def init_gemini():
    '''
    Initializes the Gemini client for embedding generation.

    The function:
    - Loads the API key from environment variables
    - Validates that the key exists
    - Creates and returns a Gemini client instance

    Notes:
        Embeddings work via the v1beta endpoint by default.

    Returns:
        genai.Client: Initialized Gemini client.

    Raises:
        RuntimeError: If GEMINI_API_KEY is missing.
    '''
    api_key = os.getenv("GEMINI_API_KEY") # Load API key from environment

    # Validate API key existence
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY in .env file")

    return genai.Client(api_key=api_key) # Create Gemini client


def get_embeddings_batch(client, chunks):
    '''
    Generates embeddings for a batch of text chunks.

    The function:
    - Validates input
    - Sends chunks to Gemini embedding model
    - Returns numerical vector representations

    Parameters:
        client (genai.Client): Initialized Gemini client.
        chunks (list[str]): List of text chunks.

    Returns:
        list[list[float]]: Embedding vectors for each chunk.
    '''
    # Return empty list if no chunks provided
    if not chunks:
        return []

    print(f"Generating embeddings for {len(chunks)} chunks...")

    # Request embeddings from Gemini
    response = client.models.embed_content(
        model="gemini-embedding-001",
        contents=chunks,
    )

    return [emb.values for emb in response.embeddings] # Extract embedding vectors

def ensure_table():
    '''
    Ensures that the document_chunks table exists in PostgreSQL.

    The function:
    - Reads the database connection URL from environment variables
    - Creates the table if it does not already exist

    Table structure:
        - id: Unique identifier
        - chunk_text: Stored text chunk
        - embedding: Numerical embedding vector
        - filename: Source document name
        - split_strategy: Chunking method used
        - created_at: Timestamp of insertion

    Raises:
        RuntimeError: If POSTGRES_URL is missing.
    '''
    url = os.getenv("POSTGRES_URL") # Load database URL from environment

    # Validate URL existence
    if not url:
        raise RuntimeError("Missing POSTGRES_URL in .env")

    # Create table if not exists
    with psycopg2.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS document_chunks (
              id BIGSERIAL PRIMARY KEY,
              chunk_text TEXT NOT NULL,
              Embedding DOUBLE PRECISION[] NOT NULL,
              Filename TEXT NOT NULL,
              split_strategy TEXT NOT NULL,
              created_at TIMESTAMPTZ DEFAULT NOW()
            );
            """)
        conn.commit()

def save_chunks_to_db(chunks, embeddings, filename, split_strategy):
    '''
    Saves text chunks and their embeddings into the PostgreSQL database.

    The function:
    - Validates database connection configuration
    - Ensures chunk and embedding alignment
    - Inserts data in batch using execute_values for performance

    Parameters:
        chunks (list[str]): List of text chunks.
        embeddings (list[list[float]]): Corresponding embedding vectors.
        filename (str): Source document name.
        split_strategy (str): Chunking method used.

    Raises:
        RuntimeError: If POSTGRES_URL is missing.
        ValueError: If chunks and embeddings lengths mismatch.
    '''
    url = os.getenv("POSTGRES_URL") # Load database URL from environment

    # Validate database URL existence
    if not url:
        raise RuntimeError("Missing POSTGRES_URL in .env")

    # Ensure chunks and embeddings are aligned
    if len(chunks) != len(embeddings):
        raise ValueError("chunks/embeddings length mismatch")

    # Prepare rows for batch insert
    rows = [(chunks[i], embeddings[i], filename, split_strategy) for i in range(len(chunks))]

    # Insert data into PostgreSQL using batch operation
    with psycopg2.connect(url) as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO document_chunks (chunk_text, embedding, filename, split_strategy)
                VALUES %s
                """,
                rows
            )
        conn.commit()

if __name__ == "__main__":
    # Load environment variables (API Keys, Database Credentials)
    load_dotenv(Path(__file__).parent / ".env")

    try:
        # Initialize the Google Gemini API client
        client = init_gemini()

##------------------------------- Insert your path file here ------------------------------->
        file_path = r"D:\Folder_for_pycharm\template-for-specific-genetic-conditions.docx" #TODO
##------------------------------------------------------------------------------------------>
        # Step 1: Text Extraction & Pre-processing
        # Extracts raw text and cleans it from redundant whitespaces/artifacts
        cleaned_text, filename = extract_and_clean_text(file_path)

        # Step 2: Dynamic Chunking
        # Applies the appropriate splitting strategy based on the file extension (PDF/DOCX)
        chunks, strategy = chunk_by_file_type(file_path, cleaned_text)

        # Step 3: Vector Embedding Generation
        # Sends chunks to Gemini text-embedding-004 in batches for efficiency
        embeddings = get_embeddings_batch(client, chunks)

        print("Successfully generated embeddings!")
        print('')
        print(f"File: {filename} | Strategy: {strategy} | Number of Embeddings: {len(embeddings)}")

        # Step 4: Database Persistence
        # Ensure the PostgreSQL table exists (with pgvector) and save the results
        ensure_table()
        save_chunks_to_db(chunks, embeddings, filename, strategy)

        print("\nProcess completed: Data successfully saved to PostgreSQL!")

    except Exception as e:
        # Catch and log any errors during the pipeline execution
        print(f"Error during execution: {e}")
