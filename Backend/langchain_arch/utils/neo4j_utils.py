import os
from neo4j import GraphDatabase
from dotenv import load_dotenv
from typing import List, Dict, Any

# Load environment variables from .env file
load_dotenv()

class Neo4jDatabase:
    """
    Utility class for interacting with a Neo4j database.

    Handles connection management, query execution, and schema retrieval.
    Reads connection details from environment variables:
    NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD, NEO4J_DATABASE
    """
    def __init__(self):
        uri = os.getenv("NEO4J_URI")
        user = os.getenv("NEO4J_USERNAME")
        password = os.getenv("NEO4J_PASSWORD")
        self.database = os.getenv("NEO4J_DATABASE", "neo4j") # Default to 'neo4j' if not set

        if not all([uri, user, password]):
            raise ValueError(
                "Neo4j connection details (NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD) "
                "must be set in environment variables."
            )

        try:
            self._driver = GraphDatabase.driver(uri, auth=(user, password))
            self._driver.verify_connectivity()
            print(f"Successfully connected to Neo4j database: {self.database} at {uri}")
        except Exception as e:
            print(f"Failed to connect to Neo4j: {e}")
            raise

    def close(self):
        """Closes the Neo4j driver connection."""
        if self._driver:
            self._driver.close()
            print("Neo4j connection closed.")

    def query(self, cypher_query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Executes a Cypher query against the database.

        Args:
            cypher_query: The Cypher query string to execute.
            params: Optional dictionary of parameters for the query.

        Returns:
            A list of records, where each record is a dictionary.
            Returns an empty list if the query fails or yields no results.
        """
        if params is None:
            params = {}
        try:
            with self._driver.session(database=self.database) as session:
                result = session.run(cypher_query, params)
                # Consume the result fully and convert records to dictionaries
                return [record.data() for record in result]
        except Exception as e:
            print(f"Error executing Cypher query: {e}")
            print(f"Query: {cypher_query}")
            print(f"Params: {params}")
            # Depending on the desired error handling, you might re-raise, return None, or empty list
            return [] # Return empty list on error for now

    def get_schema_markdown(self, schema_file_path: str) -> str | None:
        """
        Loads the graph schema from a specified Markdown file.

        Args:
            schema_file_path: The path to the Markdown file containing the schema.

        Returns:
            The content of the schema file as a string, or None if the file cannot be read.
        """
        try:
            # Adjust path relative to the project root if necessary
            # Assuming this script is run from the project root or schema_file_path is absolute
            full_path = os.path.abspath(schema_file_path)
            if not os.path.exists(full_path):
                 # Try path relative to this file's directory if not found
                base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # Go up one level from utils
                full_path = os.path.join(base_dir, schema_file_path)


            if os.path.exists(full_path):
                 with open(full_path, 'r', encoding='utf-8') as f:
                    return f.read()
            else:
                print(f"Schema file not found at expected paths: {schema_file_path} or {full_path}")
                return None
        except Exception as e:
            print(f"Error reading schema file {schema_file_path}: {e}")
            return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

# Example usage (optional, for testing)
if __name__ == '__main__':
    # Make sure you have a .env file with your Neo4j credentials
    # and neo4j_schema.md in the root or specified path
    try:
        db = Neo4jDatabase()

        # Test schema loading
        # Place neo4j_schema.md in the root directory relative to where you run this
        schema = db.get_schema_markdown("neo4j_schema.md")
        if schema:
            print("\n--- Schema Loaded ---")
            # print(schema[:500] + "...") # Print first 500 chars
        else:
            print("\n--- Schema Not Found ---")


        # Test query
        print("\n--- Testing Query ---")
        # Example: Get 5 nodes of any type
        test_query = "MATCH (n) RETURN n LIMIT 5"
        results = db.query(test_query)

        if results:
            print(f"Successfully executed query. Found {len(results)} results.")
            # for record in results:
            #     print(record) # Prints the dictionary representation
        else:
            print("Query executed, but returned no results or an error occurred.")

        db.close()

    except ValueError as ve:
        print(f"Configuration Error: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
