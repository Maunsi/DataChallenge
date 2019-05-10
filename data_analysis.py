import csv
import pandas as pd
import glob
import re
import itertools
import urllib.request
import datetime as d


def download_files():
	#This idea only works if the files keep this naming structure
	#For now we only check for files from last year until 1990
	for year in range(1990, int(d.datetime.now().year)):
		final_digits = str(year % 100) if (year % 100 >= 10) else "0"+str(year%100)
		url = "https://www.bls.gov/lau/laucnty"+final_digits+".txt"
		outputfile = "Data/Labor Force Data "+str(year)
		urllib.request.urlretrieve(url, outputfile)
	return

def file_len(filename):
	with open(filename) as file:
		for i, l in enumerate(file):
			pass
	return i + 1

def read_file(filename):
	# Convert spaces to tabs to make the conversion to dataframe easier

	modifiedfilename = "Modified"+filename
	length = file_len(filename)
	with open(filename, 'r') as inputfile:
		with open(modifiedfilename, 'w') as outputfile:
			for line in itertools.islice(inputfile, 6, length-3):
				outputfile.write(re.sub('(\s\s)+', '|', line))

	df = pd.read_csv(modifiedfilename, header=None, sep='|', names=["LAUS Code", "State FIPS Code", "County FIPS Code", 
		"County Name/State Abbreviation", "Year", "Labor Force", "Employed", "Unemployed Level", "Unemployed Rate"],
		thousands=',')
	#Tried to use na_values="N.A." to detect a nan value but did not work
	df["Labor Force"] =pd.to_numeric(df["Labor Force"], errors='coerce')
	df["Employed"] =pd.to_numeric(df["Employed"], errors='coerce')
	df["Unemployed Level"] =pd.to_numeric(df["Unemployed Level"], errors='coerce')
	df["Unemployed Rate"] =pd.to_numeric(df["Unemployed Rate"], errors='coerce')
	#I cannot average a row with any NaN value
	df.dropna()
	return df


def read_files():
	df_all_years = pd.DataFrame(columns=["LAUS Code", "State FIPS Code", "County FIPS Code", 
		"County Name/State Abbreviation", "Year", "Labor Force", "Employed", "Unemployed Level", "Unemployed Rate"])
	dataframes = []
	
	for filepath in glob.iglob("Data/*"):
		df_year = read_file(filepath)
		dataframes.append(df_year)
	df_all_years = pd.concat(dataframes) #Do i want to ignore the index?
	
	return df_all_years


def averages(df_all_years):
	print("DF ALL YEARS")
	print(df_all_years.dtypes)
	mean_county_df = df_all_years.groupby(["LAUS Code", "State FIPS Code", "County FIPS Code", "County Name/State Abbreviation"]).agg({
        'Labor Force': 'mean',  
        'Employed': 'mean',
        'Unemployed Level': 'mean',
        'Unemployed Rate': 'mean',
     }).reset_index()

	mean_state_df = df_all_years.groupby(["State FIPS Code"]).agg({
        'Labor Force': 'mean',  
        'Employed': 'mean', 
        'Unemployed Level': 'mean',
        'Unemployed Rate': 'mean',
     }).reset_index()
	#Should add a column with the statename
	return mean_county_df, mean_state_df

def write_files(mean_county_df, mean_state_df):
	mean_county_df.to_csv("OutputData/Averages By County.txt")
	#For clarity I want to create a column with the state name
	state_names = {
	2:"ALASKA", 1:"ALABAMA", 5:"ARKANSAS", 60:"AMERICAN SAMOA", 4:"ARIZONA", 6:"CALIFORNIA", 8:"COLORADO", 9:"CONNECTICUT",
	11:"DISTRICT OF COLUMBIA", 10:"DELAWARE", 12:"FLORIDA", 13:"GEORGIA", 66:"GUAM", 15:"HAWAII", 19:"IOWA", 16:"IDAHO",
	17:"ILLINOIS", 18:"INDIANA", 20:"KANSAS", 21:"KENTUCKY", 22:"LOUISIANA", 25:"MASSACHUSETTS", 24:"MARYLAND", 23:"MAINE",
	26:"MICHIGAN", 27:"MINNESOTA", 29:"MISSOURI", 28:"MISSISSIPPI", 30:"MONTANA", 37:"NORTH CAROLINA", 38:"NORTH DAKOTA",
	31:"NEBRASKA", 33:"NEW HAMPSHIRE", 34:"NEW JERSEY", 35:"NEW MEXICO", 32:"NEVADA", 36:"NEW YORK", 39:"OHIO", 40:"OKLAHOMA",
	41:"OREGON", 42:"PENNSYLVANIA", 72:"PUERTO RICO", 44:"RHODE ISLAND", 45:"SOUTH CAROLINA", 46:"SOUTH DAKOTA", 47:"TENNESSEE",
	48:"TEXAS", 49:"UTAH", 51:"VIRGINIA", 78:"VIRGIN ISLANDS", 50:"VERMONT", 53:"WASHINGTON", 55:"WISCONSIN", 54:"WEST VIRGINIA",
	56:"WYOMING"
	}
	mean_state_df["State Name"] = mean_state_df["State FIPS Code"].map(state_names)
	mean_state_df.to_csv("OutputData/Averages By State.txt")


if __name__ == '__main__':
	download_files()
	df_all_years = read_files()
	mean_county_df, mean_state_df = averages(df_all_years)
	write_files(mean_county_df, mean_state_df)