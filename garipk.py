from flask import Flask, request, jsonify
import requests
from bs4 import BeautifulSoup

app = Flask(__name__)

def build_urls(base_url, cities, makers, models, years):
    # Handle empty parameters by defaulting to [""] if not provided
    cities = cities or [""]
    makers = makers or [""]
    models = models or [""]
    years = years or [""]

    urls = []
    for city in cities:
        for maker in makers:
            for model in models:
                for year in years:
                    # Construct the URL
                    parts = [maker, model, year, f"{city}-c"]
                    url = f"{base_url}{'/'.join(part for part in parts if part)}/"
                    print(f"Constructed URL: {url}")  # Debugging statement
                    urls.append(url)
    
    return urls

def scrape_page(url, max_pages=10):
    all_results = []
    
    for page in range(1, max_pages + 1):
        paginated_url = f"{url}?page={page}"
        print(f"Fetching URL: {paginated_url}")  # Debugging statement
        response = requests.get(paginated_url)
        
        if response.status_code != 200:
            print(f"Failed to fetch {paginated_url}. Status code: {response.status_code}")
            break

        soup = BeautifulSoup(response.text, 'html.parser')
        print(f"Response from {paginated_url}: {soup.prettify()[:500]}")

        ads = soup.select("div#cat-contents")
        if not ads:
            print(f"No ads found on page {page}. Stopping pagination.")
            break

        for ad in ads:
            try:
                title_span = ad.select_one("h3 span")
                title = title_span.text.split() if title_span else []
                year, city = [info.text.strip() for info in ad.select("div.div_feat")[:2]]
                price = ad.select_one("div[style*='font-weight: bolder']").text.strip() if ad.select_one("div[style*='font-weight: bolder']") else "N/A"

                if len(title) >= 2:
                    maker, model = title[0], title[1]
                else:
                    maker, model = "Unknown", "Unknown"

                all_results.append({
                    "maker": maker,
                    "model": model,
                    "year": year,
                    "city": city,
                    "price": price
                })
            except Exception as e:
                print(f"Error parsing ad: {e}")
                continue
        
        # Stop if no more ads are found or if a stopping condition is met
        if not ads or page == max_pages:
            break

    return all_results

@app.route('/scraper/api/v1/gari_pk/scrap', methods=['GET'])
def scraper_api_gari():
    base_url = "https://www.gari.pk/used-cars/"
    
    cities = request.args.getlist('city')
    makers = request.args.getlist('maker')
    models = request.args.getlist('model')
    years = request.args.getlist('year')
    max_pages = int(request.args.get('pages', 10))  # Default to scrape up to 100 pages

    # Validate parameters
    if not makers:
        return jsonify({"error": "Maker parameter is required."}), 400

    urls = build_urls(base_url, cities, makers, models, years)
    data = []
    for url in urls:
        data.extend(scrape_page(url, max_pages=max_pages))

    if not data:
        return jsonify({"error": "No results found for the specified filters."}), 404

    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5001)
