import os
import pandas as pd

from scraper import scraper_df


# Convert list to dataframe

base_url = 'https://www.rightmove.co.uk/property-for-sale/find.html'
locations = ["REGION%5E79192", "REGION%5E61379", "REGION%5E87490", "REGION%5E61317", "REGION%5E61378", "REGION%5E61316"]

scraped_df= scraper_df(base_url, locations)
path= os.path.join("/workspaces/data_engineering/data", 'housing.csv')

# convert DataFrame to csv

scraped_df.to_csv(path)