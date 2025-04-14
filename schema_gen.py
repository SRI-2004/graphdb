from neo4j import GraphDatabase
from collections import defaultdict
import os

# === CONFIGURATION ===
NEO4J_URI = "neo4j+s://2557c6ca.databases.neo4j.io"
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "pg8JVNkM25tYoxJA9Gg4orjBu-mX0S5GaNAYJ8Xv2mU")

# === CONNECTOR CLASS ===
class Neo4jSchemaExtractor:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_node_schema(self):
        query = """
        CALL db.schema.nodeTypeProperties() 
        YIELD nodeType, propertyName, propertyTypes 
        RETURN nodeType, propertyName, propertyTypes
        """
        with self.driver.session() as session:
            result = session.run(query)
            node_schema = defaultdict(list)
            for record in result:
                label = record["nodeType"]
                if not label or label.startswith("_"): continue # Skip internal/empty labels
                types = record["propertyTypes"]
                if isinstance(types, list):
                    types_str = "|".join(types)
                else:
                    types_str = str(types)
                prop = f"{record['propertyName']} : {types_str}"
                node_schema[label].append(prop)
        return node_schema

    def get_relationship_schema(self):
        query = """
        CALL db.schema.relTypeProperties() 
        YIELD relType, propertyName, propertyTypes 
        RETURN relType, propertyName, propertyTypes
        """
        with self.driver.session() as session:
            result = session.run(query)
            rel_schema = defaultdict(list)
            for record in result:
                rel = record["relType"]
                if not rel or rel.startswith("_"): continue # Skip internal/empty rel types
                property_name = record["propertyName"]
                types = record["propertyTypes"]
                if property_name is None:
                    continue
                if isinstance(types, list):
                    types_str = "|".join(types)
                else:
                    types_str = str(types)
                prop = f"{property_name} : {types_str}"
                rel_schema[rel].append(prop)
        return rel_schema

    def get_relationship_structure(self):
        """Fetches relationship structures using db.schema.visualization."""
        query = "CALL db.schema.visualization()"
        rel_structure = defaultdict(set)
        with self.driver.session() as session:
            result = session.run(query)
            data = result.data()
            if not data or 'nodes' not in data[0] or 'relationships' not in data[0]:
                print("Warning: Could not retrieve nodes or relationships from db.schema.visualization().")
                return rel_structure
            
            relationships = data[0]['relationships']
            nodes_map = {node['id']: node for node in data[0]['nodes'] if 'id' in node}
            if len(nodes_map) != len(data[0]['nodes']):
                print(f"Warning: Skipped {len(data[0]['nodes']) - len(nodes_map)} nodes missing 'id' key in visualization output.")

            for rel in relationships:
                # Expecting tuple: (start_node_dict, rel_type_string, end_node_dict)
                if not isinstance(rel, tuple) or len(rel) != 3:
                    print(f"Warning: Skipping unexpected item found in relationships list: {type(rel)} - {rel}")
                    continue
                    
                # Access elements by index
                start_node_info = rel[0]
                rel_type = rel[1]
                end_node_info = rel[2]

                # Ensure components are dictionaries and string
                if not isinstance(start_node_info, dict) or not isinstance(end_node_info, dict) or not isinstance(rel_type, str):
                    print(f"Warning: Skipping relationship tuple with unexpected component types: {rel}")
                    continue
                    
                if not rel_type or not start_node_info or not end_node_info:
                    print(f"Warning: Skipping relationship tuple due to empty components: {rel}")
                    continue
                    
                # FIX: Get label from 'name' key, not 'labels' key
                start_label = start_node_info.get('name')
                end_label = end_node_info.get('name')

                # Create lists containing the single label if found
                start_labels = [start_label] if start_label else []
                end_labels = [end_label] if end_label else []
                
                start_label_str = ":".join([f"`{l}`" for l in start_labels if l and not l.startswith("_")])
                end_label_str = ":".join([f"`{l}`" for l in end_labels if l and not l.startswith("_")])
                
                if start_label_str and end_label_str and not rel_type.startswith("_"):
                    rel_structure[rel_type].add((start_label_str, end_label_str))
                
        return rel_structure

    def get_node_counts(self):
        with self.driver.session() as session:
            labels_result = session.run("CALL db.labels() YIELD label RETURN label")
            counts = {}
            for record in labels_result:
                label = record["label"]
                if not label or label.startswith("_"): continue # Skip internal/empty labels
                count_query = f"MATCH (n:`{label}`) RETURN count(n) AS count"
                count = session.run(count_query).single()["count"]
                counts[label] = count
        return counts

    def get_indexes(self):
        query = "SHOW INDEXES"
        with self.driver.session() as session:
            result = session.run(query)
            indexes = []
            for record in result:
                try:
                    # Check for None before accessing list elements
                    label_or_type = record['labelsOrTypes'][0] if record['labelsOrTypes'] else ''
                    properties_str = ', '.join(record['properties']) if record['properties'] else ''
                    index_info = f"{record['name']} on {record['entityType']} `{label_or_type}` ({properties_str})"
                    indexes.append(index_info)
                except (TypeError, IndexError, KeyError) as e:
                    print(f"Warning: Could not parse index record: {record}. Error: {e}")
                    continue # Skip malformed records
        return indexes

    def get_constraints(self):
        query = "SHOW CONSTRAINTS"
        with self.driver.session() as session:
            result = session.run(query)
            constraints = []
            for record in result:
                try:
                    # Check for None before accessing list elements
                    label_or_type = record['labelsOrTypes'][0] if record['labelsOrTypes'] else ''
                    properties_str = ', '.join(record['properties']) if record['properties'] else ''
                    constraint_info = f"{record['name']}: {record['type']} on {record['entityType']} `{label_or_type}` ({properties_str})"
                    constraints.append(constraint_info)
                except (TypeError, IndexError, KeyError) as e:
                    print(f"Warning: Could not parse constraint record: {record}. Error: {e}")
                    continue # Skip malformed records
        return constraints

    def generate_markdown(self, node_schema, rel_properties, rel_structure, node_counts, indexes, constraints, output_path="neo4j_schema.md"):
        """Generates the markdown schema file, including relationship structures."""
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("# 🧠 Neo4j Graph Schema\n\n")

            f.write("## 📊 Node Counts\n\n")
            for label, count in sorted(node_counts.items()):
                f.write(f"- `{label}`: {count}\n")
            f.write("\n")

            f.write("## 🟢 Node Types & Properties\n\n")
            for label, props in sorted(node_schema.items()):
                f.write(f"### `{label}`\n")
                for prop in sorted(props):
                    f.write(f"- {prop}\n")
                f.write("\n")

            f.write("## 🔗 Relationship Types, Structures & Properties\n\n")
            # Iterate through structures found by visualization
            all_rel_types_structured = sorted(rel_structure.keys())

            if not all_rel_types_structured:
                 f.write("_(No relationship structures found using db.schema.visualization())_\n\n")
            else:
                for rel_type in all_rel_types_structured:
                    structures = sorted(list(rel_structure[rel_type]))
                    props = rel_properties.get(rel_type, [])
                    
                    # Write structure(s)
                    for start_label, end_label in structures:
                        f.write(f"### `({start_label})-[:{rel_type}]->({end_label})`\n")
                    
                    # Write properties for this relationship type
                    if props:
                        f.write("**Properties:**\n")
                        for prop in sorted(props):
                            f.write(f"- {prop}\n")
                    else:
                        f.write("*(No properties)*\n")
                    f.write("\n")
            
            # Add remaining relationship types that only have properties (if any)
            # This handles cases where visualization might miss some due to sampling/limits
            remaining_prop_rels = sorted([rt for rt in rel_properties if rt not in rel_structure])
            if remaining_prop_rels:
                f.write("### Other Relationships (Properties Only)\n")
                f.write("_(These relationships had properties defined but no structure was found via db.schema.visualization)_\n\n")
                for rel_type in remaining_prop_rels:
                     props = rel_properties[rel_type]
                     f.write(f"#### `{rel_type}`\n")
                     f.write("**Properties:**\n")
                     for prop in sorted(props):
                         f.write(f"- {prop}\n")
                     f.write("\n")

# === RUN SCRIPT ===
if __name__ == "__main__":
    print("--- Running schema_gen.py ---") # Added to check script start
    # Ensure NEO4J_PASSWORD is set in environment or default is used
    password = os.getenv("NEO4J_PASSWORD")
    if not password:
         print("Warning: NEO4J_PASSWORD environment variable not set. Using default from script.")
         password = NEO4J_PASSWORD # Use the one defined in the script

    # === INSTANTIATE CONNECTOR ===
    connector = Neo4jSchemaExtractor(NEO4J_URI, NEO4J_USER, password)

    # === EXTRACT SCHEMA ===
    node_schema = connector.get_node_schema()
    rel_properties = connector.get_relationship_schema()
    rel_structure = connector.get_relationship_structure()
    node_counts = connector.get_node_counts()
    indexes = connector.get_indexes()
    constraints = connector.get_constraints()

    # === GENERATE MARKDOWN ===
    connector.generate_markdown(node_schema, rel_properties, rel_structure, node_counts, indexes, constraints)

    # === CLOSE CONNECTOR ===
    connector.close()