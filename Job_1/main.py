from flask import Flask, jsonify, request
import requests
import os
import json
import shutil

app = Flask(__name__)
AUTH_TOKEN = os.environ['AUTH_TOKEN']

@app.route('/fetch_api_data', methods=['POST'])
def fetch_api_data():

    
    api_url = ('https://fake-api-vycpfa6oca-uc.a.run.app/sales')

   
    headers = {'Authorization':AUTH_TOKEN}

   
    raw_dir = request.json.get('raw_dir')
    date = request.json.get('date')

    
    os.makedirs(raw_dir, exist_ok=True)
    
   
    for filename in os.listdir(raw_dir):
        file_path = os.path.join(raw_dir, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
                print(f'Не вдалося видалити {file_path}. Причина: {e}')
  
    n=1
    while True:
    
     
      response = requests.get(api_url,params={'date': date, 'page': n}, headers=headers)
      
      if response.status_code != 200:
        return jsonify({"message": "Вивантажено "+str(n-1)+" сторінки"}), response.status_code
       
      data = response.json()
    
        
      filename = os.path.join(raw_dir, f"sales_"+date+"_"+str(n)+".json")
      n=n+1  
        
      with open(filename, 'w') as f:
          json.dump(data, f, indent=4)
      
if __name__ == '__main__':
    app.run(debug=True, port=8081)