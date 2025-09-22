#!/usr/bin/env python3
"""
Simple local server to run the expected results API
"""
from flask import Flask, request, jsonify
import json
from app import handler

app = Flask(__name__)

@app.route('/mock-api', methods=['POST'])
def mock_api():
    """Mock API endpoint that matches the expected results client"""
    try:
        # Get the request data
        data = request.get_json()
        
        # Create a mock event for the handler
        event = {
            "body": json.dumps(data)
        }
        
        # Call the handler
        result = handler(event, None)
        
        # Return the response
        return jsonify(json.loads(result['body'])), result['statusCode']
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    print("🚀 Starting local expected results API server...")
    print("📍 API will be available at: http://localhost:3000/mock-api")
    app.run(host='0.0.0.0', port=3000, debug=True)
