# 🚖 NYC Yellow Taxi Trip Data Analysis  

## 📌 Overview  
This project analyzes **NYC Yellow Taxi Trip Data** from **January 2015** using **Apache Spark** and **Python** in a **Databricks environment**.  
The analysis includes **data cleaning, feature engineering, descriptive statistics, and visualization** to uncover insights about NYC taxi rides.  

## 📂 Dataset  
- **Source:** [Kaggle - NYC Yellow Taxi Trip Data](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data?resource=download)  
- **Description:** Contains trip details such as pickup/drop-off locations, fare amounts, passenger counts, and timestamps.  
- **Files Used:**  
  - `yellow_tripdata_2015_01.csv`  

## 🚀 Features and Methodology  

### 🔍 Data Preprocessing  
- **Schema Inspection**: Display column data types and missing values.  
- **Filtering & Cleaning**: Removed **null values**, trips with **zero distance**, and **negative fares**.  
- **Timestamp Conversion**: Converted pickup and dropoff times into **timestamp format**.  
- **Feature Engineering**:  
  - **Trip Duration** (`minutes`)  
  - **Trip Speed** (`miles per hour`)  
  - **Pickup Hour & Day of Week**  

### 📊 Exploratory Data Analysis (EDA)  
- **Descriptive Statistics**: Summary of **trip distances, fares, and passenger counts**.  
- **Busiest Pickup Hours**: Determining when NYC taxis are most active.  
- **Geospatial Analysis**: Identifying areas with **high average fares**.  
- **Trip Speed & Distance Distribution**: Understanding ride efficiency.  

### 📈 Visualizations  
✅ **Trip Distance Distribution**  
✅ **Busiest Pickup Hours**  
✅ **Average Fares per Hour**  
✅ **Trip Duration Trends by Day**  
✅ **Trip Duration vs. Hour of Day**  



## 🛠 Technologies Used  
- **Databricks**  
- **Apache Spark (PySpark)**  
- **Python**  
- **Pandas & NumPy**  
- **Matplotlib & Seaborn**  

## 📥 Installation & Setup  

### 1️⃣ Install Required Libraries  
```bash
pip install pyspark pandas numpy matplotlib seaborn
```
### 2️⃣ Load Dataset in Databricks
```python
data2015_1 = spark.read.csv('dbfs:/FileStore/yellow_tripdata_2015_01.csv', header=True, inferSchema=True)
```
3️⃣ Run the Analysis
- Execute the Databricks Notebook or Python script to perform data processing, analysis, and visualization.

🛠️ Future Improvements
- Implement geospatial clustering to analyze popular taxi zones.
- Use machine learning models to predict trip durations and fares.
- Integrate real-time streaming analytics for live NYC taxi monitoring.
