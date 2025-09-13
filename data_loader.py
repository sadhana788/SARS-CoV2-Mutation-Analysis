from Bio import SeqIO
import pandas as pd
import sqlalchemy as sa

# 1. Database Connection
# Yeh ek naya database file banayega jiska naam covid_data.db hoga
engine = sa.create_engine('sqlite:///covid_data.db')

# 2. Create Table
with engine.connect() as conn:
    conn.execute(sa.text("""
    CREATE TABLE IF NOT EXISTS variants (
        accession_id VARCHAR PRIMARY KEY,
        sequence TEXT,
        host VARCHAR,
        location VARCHAR,
        collection_date DATE
    );
    """))

# 3. Read FASTA file and load into a DataFrame
records = []
for record in SeqIO.parse("COVID_sequence.fasta", "fasta"):
    description_parts = record.description.split(" ")
    
    # Extract metadata from the description line
    accession_id = description_parts[0]
    host = 'N/A'
    location = 'N/A'
    collection_date = 'N/A'
    
    # Simple parsing logic based on description format
    for part in description_parts:
        if part.startswith('host='):
            host = part.split('=')[1]
        if part.startswith('location='):
            location = part.split('=')[1]
        if part.startswith('collection_date='):
            collection_date = part.split('=')[1]
            
    records.append({
        'accession_id': accession_id,
        'sequence': str(record.seq),
        'host': host,
        'location': location,
        'collection_date': collection_date
    })

df = pd.DataFrame(records)

# 4. Load DataFrame into SQL Table
df.to_sql('variants', engine, if_exists='replace', index=False)

print("Data loaded successfully into covid_data.db!")