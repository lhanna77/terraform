import json
from pathlib import Path

def generate_schema(data):
    if isinstance(data, dict):
        schema = {
            "type": "object",
            "properties": {},
            "required": []
        }
        for key, value in data.items():
            schema["properties"][key] = generate_schema(value)
            schema["required"].append(key)
        return schema
    elif isinstance(data, list):
        return {
            "type": "array",
            "items": generate_schema(data[0]) if data else {}
        }
    else:
        return {"type": type(data).__name__}

# Path to the JSONL file
jsonl_file_path = Path(r'C:\Users\lehannah\OneDrive - Monster_AD\Monster Files\Learning\Terraform\Udemy\terraform\files\json\nested.json')

# Path to save the generated JSON Schema file
schema_file_path = Path(r'C:\Users\lehannah\OneDrive - Monster_AD\Monster Files\Learning\Terraform\Udemy\terraform\bqschemas\schema.json')   

# Read the JSONL data and extract a sample JSON record
with open(jsonl_file_path, "r") as jsonl_file:
    sample_record = jsonl_file.readline()

# Parse the sample JSON record
sample_json = json.loads(sample_record)

# Generate the JSON Schema
schema = generate_schema(sample_json)

# Save the generated JSON Schema to a file
with open(schema_file_path, "w") as schema_file:
    json.dump(schema, schema_file, indent=2)

print("JSON Schema file has been generated successfully.")
