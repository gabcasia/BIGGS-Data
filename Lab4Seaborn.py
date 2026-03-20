# ===============================
# COVID Data Visualization (Seaborn)
# ===============================

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# -------------------------------
# 1. Load Dataset
# -------------------------------
df = pd.read_csv("owid-covid-data.csv")

# -------------------------------
# 2. Data Preprocessing
# -------------------------------
df['date'] = pd.to_datetime(df['date'], errors='coerce')

# Filter year 2021
df = df[df['date'].dt.year == 2021]

# Select relevant columns
df = df[['location', 'date', 'new_cases', 'new_deaths', 'population']]

# Fill missing values
df = df.fillna(0)

# Filter countries
countries = ['United States', 'India', 'Brazil', 'Philippines']
df = df[df['location'].isin(countries)]

# Create month column
df['month'] = df['date'].dt.month

# Aggregate (same as Spark)
monthly = df.groupby(['location', 'month']).agg({
    'new_cases': 'sum',
    'new_deaths': 'sum'
}).reset_index()

# -------------------------------
# 3. Styling
# -------------------------------
sns.set_style("whitegrid")
sns.set_context("talk")

# -------------------------------
# 4. VISUALIZATIONS
# -------------------------------

# 1️⃣ Line Plot
plt.figure()
sns.lineplot(data=monthly, x='month', y='new_cases', hue='location')
plt.title("Monthly COVID Cases (2021)")
plt.xlabel("Month")
plt.ylabel("Cases")
plt.show()

# 2️⃣ Bar Plot
plt.figure()
sns.barplot(data=monthly, x='month', y='new_cases', hue='location')
plt.title("Monthly Cases Comparison")
plt.xlabel("Month")
plt.ylabel("Cases")
plt.show()

# 3️⃣ Scatter Plot
plt.figure()
sns.scatterplot(data=df, x='new_cases', y='new_deaths', hue='location')
plt.title("Cases vs Deaths")
plt.xlabel("New Cases")
plt.ylabel("New Deaths")
plt.show()

# 4️⃣ Histogram
plt.figure()
sns.histplot(data=df, x='new_cases', bins=50, color='blue')
plt.title("Distribution of New Cases")
plt.xlabel("Cases")
plt.ylabel("Frequency")
plt.show()

# 5️⃣ Box Plot
plt.figure()
sns.boxplot(data=df, x='location', y='new_cases')
plt.title("Cases Distribution by Country")
plt.xlabel("Country")
plt.ylabel("Cases")
plt.show()
