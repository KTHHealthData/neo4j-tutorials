# Simple script template to read from a CSV and insert into a Neo4j database

import csv
import uuid
from neo4j import GraphDatabase


URI = "neo4j://localhost:7687"
USER = "neo4j" # Note that it will always be neo4j for the free/community edition of neo4j
PASS = "pass" # password

driver = GraphDatabase.driver(URI, auth=(USER, PASS))

# Verify Connectivity
driver.verify_connectivity()


CSV_FILE = 'my_file.csv'
CSV_SEP = ';' # separator used in the CSV file. Usually ","


def write_library(tx,id,library,sigel,extern_id,kommun_code,library_type,stad):
    result = tx.run("""
        MATCH (k:Kommun)
        WHERE k.kommun = $kommun_code
        MATCH (bt:bib_type)
        WHERE bt.type = $library_type
        MATCH (bs:bib_stad)
        WHERE bs.sv = $stad
        MERGE(b:bibliotek{id:$id})
        SET b.bibliotek = $library
        SET b.sigel = $sigel
        SET b.extern_id = $extern_id
        MERGE (b)-[:LOCATED_IN]->(k)
        MERGE (b)-[:LIBRARY_TYPE]->(bt)
        MERGE (b)-[:LIBRARY_STAD]->(bs)
        RETURN b
    """,id=id,kommun_code=kommun_code,library_type=library_type,stad=stad,library=library,sigel=sigel,extern_id=extern_id)
    record = result.single()
    summary = result.consume()
    return record, summary


def process_library(row):
    library = row[0]
    library_sigel = row[1]
    library_type = row[2]
    library_kommun = row[3]
    library_stad = row[4]
    library_extren_id = row[5]
    
    id = str(uuid.uuid4())
    if library_stad == "-1":
        library_stad = ""

    with driver.session() as session:
            result, summary = session.execute_write(write_library,id,library,library_sigel,library_extren_id,library_kommun,library_type,library_stad)
            print("Nodes created: " + str(summary.counters.nodes_created))
            #print("Query: " + str(summary.query))
            #print("Parameters: "+ str(summary.parameters))
    session.close()



def load_libraries():
    with open(CSV_FILE, "r") as file:
        csvFile = csv.reader(file, CSV_SEP)
        line_count = 0
        for row in csvFile:
            if line_count < 1:
                line_count += 1
                continue
            else:
                # Read a row and give a unique case number
                process_library(row)
                line_count += 1
                print("Processing line: " + str(line_count))
    file.close()


load_libraries()
print("COMPLETED!")
