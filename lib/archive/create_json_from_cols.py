
def main():
    
    final_str = ""    
    input = [
        {"name": "messageId", "type": "STRING", "mode": "REQUIRED", "description": "messageId"},
        
        {"name": "attributes", "type": "RECORD", "mode": "NULLABLE", "description": "attributes"},
        
        {"name": "conversionSchemaPath", "type": "STRING", "mode": "NULLABLE","description": "conversionSchemaPath"},
        {"name": "data", "type": "BYTES", "mode": "NULLABLE","description": "data"}
    ]

    for i in input:

        name = i["name"]
        type = i["type"]
        mode = i["mode"]
        description = i["description"]
        
        str_record_structure = """{
                    "name": "<replace name>",
                    "type": "<replace type>",
                    "mode": "<replace mode>",
                    "description": "<replace description>",
                    "fields": {

                    }
                },"""

        str_record_replace = str_record_structure.replace("<replace name>",name).replace("<replace type>",type).replace("<replace mode>",mode).replace("<replace description>",description)
        str_replace = str_record_replace.replace(""",
                    "fields": {

                    }""",'')

        if i["type"] == "RECORD":
            #print(str_record_replace)
            final_str += str_record_replace
        else:
            #print(str_replace)
            final_str += str_replace
            
    return final_str

final = """[
    {
        "fields": [

                <replace>

        ],
        "mode": "REQUIRED",
        "name": "DataLake",
        "type": "RECORD"
    }

] """

print(final.replace("<replace>",main()))


