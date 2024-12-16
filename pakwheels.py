from flask import Flask, request, jsonify
from bs4 import BeautifulSoup
import requests
import hashlib

app = Flask(__name__)

def generate_hash(ad):
    """Generate a unique hash for each ad based on its content."""
    ad_str = f"{ad['maker']}{ad['model']}{ad['year']}{ad['city']}{ad['price']}"
    return hashlib.md5(ad_str.encode()).hexdigest()

def fetch_data_for_url(url, pages=5):
    all_results = []
    for page in range(1, pages + 1):
        paginated_url = f"{url}?page={page}"
        print(f"Fetching URL: {paginated_url}")  # Debug: Print URL with pagination

        response = requests.get(paginated_url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            ads = soup.select(".search-listing, .well.search-list, .ads-list, .classified-listing")

            print(f"Fetched {len(ads)} ads from URL: {paginated_url}")  # Debug: Print number of ads fetched

            for ad in ads:
                try:
                    title = ad.select_one(".search-title h3, .card-title, .item-title").text.strip()
                    price = ad.select_one(".price-details, .price").text.strip()
                    location = ad.select_one(".search-vehicle-info.fs13 li, .car-location, .location").text.strip()

                    year_elements = ad.select(".search-vehicle-info-2.fs13 li, .car-detail li, .details li")
                    year = next((el.text.strip() for el in year_elements if el.text.strip().isdigit()), "")

                    title_parts = title.split()
                    ad_maker = title_parts[0]
                    ad_model = title_parts[1]

                    all_results.append({
                        "maker": ad_maker,
                        "model": ad_model,
                        "year": year,
                        "city": location,
                        "price": price
                    })
                except Exception as e:
                    print(f"Error parsing ad: {e}")

    return all_results

def fetch_combined_data(cities=None, makers=None, models=None, year=None, pages=7):
    base_url = "https://www.pakwheels.com/used-cars/search/-"
    all_results = []

    # Construct URLs based on combinations of parameters
    urls = []

    if cities:
        for city in cities:
            city_url = f"{base_url}/ct_{city}"
            if makers:
                for maker in makers:
                    maker_url = f"{city_url}/mk_{maker}"
                    if models:
                        for model in models:
                            model_url = f"{maker_url}/md_{model}"
                            if year:
                                urls.append(f"{model_url}/yr_{year}/")
                            else:
                                urls.append(f"{model_url}/")
                    elif year:
                        urls.append(f"{maker_url}/yr_{year}/")
                    else:
                        urls.append(f"{maker_url}/")
            elif models:
                for model in models:
                    model_url = f"{city_url}/md_{model}"
                    if year:
                        urls.append(f"{model_url}/yr_{year}/")
                    else:
                        urls.append(f"{model_url}/")
            elif year:
                urls.append(f"{city_url}/yr_{year}/")
            else:
                urls.append(city_url)

    elif makers:
        for maker in makers:
            maker_url = f"{base_url}/mk_{maker}"
            if models:
                for model in models:
                    model_url = f"{maker_url}/md_{model}"
                    if year:
                        urls.append(f"{model_url}/yr_{year}/")
                    else:
                        urls.append(f"{model_url}/")
            elif year:
                urls.append(f"{maker_url}/yr_{year}/")
            else:
                urls.append(f"{maker_url}/")

    elif models:
        for model in models:
            model_url = f"{base_url}/md_{model}"
            if year:
                urls.append(f"{model_url}/yr_{year}/")
            else:
                urls.append(f"{model_url}/")

    elif year:
        urls.append(f"{base_url}/yr_{year}/")

    else:
        urls.append(base_url)  # No parameters, default URL

    # Fetch data for each URL
    for url in urls:
        print(f"Fetching URL: {url}")  # Debug: Print the URL being fetched
        results = fetch_data_for_url(url, pages=pages)
        all_results.extend(results)

    # Deduplicate results
    seen_hashes = set()
    deduplicated_results = []
    for result in all_results:
        ad_hash = generate_hash(result)
        if ad_hash not in seen_hashes:
            seen_hashes.add(ad_hash)
            deduplicated_results.append(result)

    return deduplicated_results

@app.route('/scraper/api/v1/pakwheels/scrap', methods=['GET'])
def scraper_api():
    cities = request.args.getlist('city')
    makers = request.args.getlist('maker')
    models = request.args.getlist('model')
    year = request.args.get('year')
    pages = int(request.args.get('pages', 7))  # Number of pages to scrape, default to 5

    # Debug: Print the parameters
    print(f"Cities: {cities}, Makers: {makers}, Models: {models}, Year: {year}, Pages: {pages}")

    data = fetch_combined_data(cities=cities, makers=makers, models=models, year=year, pages=pages)

    if not data:
        return jsonify({"error": "No results found for the specified filters."}), 404

    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5000)
