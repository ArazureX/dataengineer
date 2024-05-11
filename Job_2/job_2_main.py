from fastavro import writer, parse_schema
from flask import Flask, request, jsonify
import json
import os
from datetime import datetime

app = Flask(__name__)

# Визначте схему для файлу AVRO
schema = {
  "type": "record",
  "name": "Purchase",
  "namespace": "com.example",
  "fields": [
    {
      "name": "client",
      "type": "string"
    },
    {
      "name": "purchase_date",
      "type": {
        "type": "int",
        "logicalType": "date"
      }
    },
    {
      "name": "product",
      "type": "string"
    },
    {
      "name": "price",
      "type": "float"
    }
  ]
}


parsed_schema = parse_schema(schema)

@app.route('/convert', methods=['POST'])
def convert_json_to_avro():
    
    raw_dir = request.json.get('raw_dir')
    stg_dir = request.json.get('stg_dir')
    
    os.makedirs(stg_dir, exist_ok=True)
    
    
    json_files = [f for f in os.listdir(raw_dir) if f.endswith('.json')]

    for json_file in json_files:
        
        json_file_path = os.path.join(raw_dir, json_file)
        avro_file_path = os.path.join(stg_dir, json_file.replace('.json', '.avro'))

        
        with open(json_file_path, 'r') as jf:
            json_data_list = json.load(jf)

            
            for json_data in json_data_list:
                
                purchase_date_str = json_data['purchase_date']
                purchase_date_obj = datetime.strptime(purchase_date_str, '%Y-%m-%d')
                purchase_date_int = (purchase_date_obj - datetime(1970, 1, 1)).days
                json_data['purchase_date'] = purchase_date_int

                
                with open(avro_file_path, 'wb') as af:
                    writer(af, parsed_schema, [json_data])

    return jsonify({'message': 'Конвертація успішно завершена'}), 201

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=8082)
