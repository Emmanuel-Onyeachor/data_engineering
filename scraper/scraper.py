# Import libraries

import requests
from bs4 import BeautifulSoup
import pandas as pd

# Scrape the data

def scraper_df(base_url, locations):

    homes = []

    for location in locations:
        url = f"{base_url}?propertyTypes=&includeSSTC=false&mustHave=&dontShow=&furnishTypes=&keywords=&locationIdentifier={location}"

        for page_index in range(0, 985, 24):
            url2 = f"{url}&index={page_index}"
            try:
                response = requests.get(url2)
                response.raise_for_status()  # Raise an error for bad responses
                soup = BeautifulSoup(response.text, 'html.parser')

                homes.extend([{
                    'address': each.find('address').get_text(strip=True),
                    'price': each.find('div', class_="propertyCard-priceValue").get_text(strip=True),
                    'listing_date': each.find('span', class_="propertyCard-branchSummary-addedOrReduced").get_text,
                    'realtor': each.find('span', class_="propertyCard-branchSummary-branchName").get_text,
                    'phone': each.find('a', class_="propertyCard-contactsPhoneNumber").get_text,
                    'num_rooms': each.find('h2', class_="propertyCard-title").get_text(strip=True)
                } for each in soup.find_all('div', class_="propertyCard-wrapper")])

            except requests.RequestException as e:
                print(f"Request failed: {e}")
    
    scraped_df = pd.DataFrame(homes)

    return scraped_df


if __name__ == "__main__":
    
    scraper_df(base_url, locations)
    

