$url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
Invoke-WebRequest -Uri $url -OutFile "owid-covid-data.csv"
