# DS501_Project

#Ideas
# - Theme #3: Merge 2 databases and report on findings of how they enhanced each other
#1. Merge House price and House characteristics Data

#First dataset link: https://www.kaggle.com/datasets/jamshidahmadov/california-house-information

#Second dataset link: https://www.kaggle.com/datasets/quantbruce/californiahouseprices

#Link for the api: https://geocoding.geo.census.gov/geocoder/
#Click "Find Geographies", "Geographic Coordinator"

#We are matching the houses in dataset 1 with the houses in data set 2 using the geocoding API. Then we will compare the changes in house prices over the years.
#Data set 1 will drop all rows/locations based on longitude/latitude that are not in Los Angeles, San Diego, or Orange County.
#Data set 2 will drop all rows/locations based on cities that are not in the counties mentioned above. 
#Bonus: We could create and implement a price change prediction
