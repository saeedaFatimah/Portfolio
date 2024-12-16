from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

def fetch_data_from_pakwheels(cities, makers, models, years):
    base_url = "http://127.0.0.1:5000/scraper/api/v1/pakwheels/scrap"
    params = {
        "city": cities if cities else [],  # Default to empty list if not provided
        "maker": makers if makers else [],  # Default to empty list if not provided
        "model": models if models else [],  # Default to empty list if not provided
        "year": years if years else []  # Default to empty list if not provided
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        return []

def fetch_data_from_garipk(cities, makers, models, years):
    base_url = "http://127.0.0.1:5001/scraper/api/v1/gari_pk/scrap"
    params = {
        "city": cities if cities else [],  # Default to empty list if not provided
        "maker": makers if makers else [],  # Default to empty list if not provided
        "model": models if models else [],  # Default to empty list if not provided
        "year": years if years else []  # Default to empty list if not provided
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        return []

@app.route('/scraper/api/v1/competitor_analysis', methods=['GET'])
def competitor_analysis():
    cities = request.args.getlist('city')
    makers = request.args.getlist('maker')
    models = request.args.getlist('model')
    years = request.args.getlist('year')

    # Fetch data from both APIs
    pakwheels_data = fetch_data_from_pakwheels(cities, makers, models, years)
    garipk_data = fetch_data_from_garipk(cities, makers, models, years)

    # Create a dictionary for quick lookups for PakWheels data
    pakwheels_dict = {}
    for item in pakwheels_data:
        key = (item['maker'].lower(), item['model'].lower(), item['city'].lower(), item['year'])
        pakwheels_dict[key] = item['price']

    # Compare data
    comparison_results = []
    for item in garipk_data:
        key = (item['maker'].lower(), item['model'].lower(), item['city'].lower(), item['year'])
        if key in pakwheels_dict:
            comparison_results.append({
                "maker": item['maker'],
                "model": item['model'],
                "year": item['year'],
                "city": item['city'],
                "garipk_price": item['price'],
                "pakwheels_price": pakwheels_dict[key]
            })

    if not comparison_results:
        return jsonify({"message": "No matching results found for the specified filters."}), 404

    return jsonify(comparison_results)

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5002)
