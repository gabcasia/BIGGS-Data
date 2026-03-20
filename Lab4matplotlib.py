# ===============================
# COVID Data Visualization (Matplotlib)
# ===============================

import pandas as pd
import matplotlib.pyplot as plt

# -------------------------------
# 1. Load Dataset
# -------------------------------
df = pd.read_csv("owid-covid-data.csv")

# -------------------------------
# 2. Data Preprocessing
# -------------------------------
df['date'] = pd.to_datetime(df['date'], errors='coerce')

# Filter 2021
df = df[df['date'].dt.year == 2021]

# Select columns
df = df[['location', 'date', 'new_cases', 'new_deaths', 'population']]

# Fill missing values
df = df.fillna(0)

# Filter countries
countries = ['United States', 'India', 'Brazil', 'Philippines']
df = df[df['location'].isin(countries)]

# Create month column
df['month'] = df['date'].dt.month

# Aggregate
monthly = df.groupby(['location', 'month']).agg({
    'new_cases': 'sum',
    'new_deaths': 'sum'
}).reset_index()

# -------------------------------
# 3. VISUALIZATIONS
# -------------------------------

# 1️⃣ Line Plot
plt.figure()

for country in countries:
    data = monthly[monthly['location'] == country]
    plt.plot(data['month'], data['new_cases'], label=country)

plt.title("Monthly COVID Cases (2021)")
plt.xlabel("Month")
plt.ylabel("Cases")
plt.legend()
plt.show()


# 2️⃣ Bar Plot
plt.figure()

for i, country in enumerate(countries):
    data = monthly[monthly['location'] == country]
    plt.bar(
        [m + i*0.2 for m in data['month']], 
        data['new_cases'], 
        width=0.2, 
        label=country
    )

plt.title("Monthly Cases Comparison")
plt.xlabel("Month")
plt.ylabel("Cases")
plt.legend()
plt.show()


# 3️⃣ Scatter Plot
plt.figure()

for country in countries:
    data = df[df['location'] == country]
    plt.scatter(data['new_cases'], data['new_deaths'], label=country)

plt.title("Cases vs Deaths")
plt.xlabel("New Cases")
plt.ylabel("New Deaths")
plt.legend()
plt.show()


# 4️⃣ Histogram
plt.figure()

plt.hist(df['new_cases'], bins=50)
plt.title("Distribution of New Cases")
plt.xlabel("Cases")
plt.ylabel("Frequency")
plt.show()


# 5️⃣ Box Plot
plt.figure()

data_to_plot = [df[df['location'] == c]['new_cases'] for c in countries]

plt.boxplot(data_to_plot, labels=countries)

plt.title("Cases Distribution by Country")
plt.ylabel("Cases")
plt.show()
