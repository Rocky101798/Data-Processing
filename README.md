# 🚗 Bright Motors Car Sales Analysis

## 📌 Project Overview

This project analyzes vehicle sales data from Bright Motors to generate actionable business insights for the new Head of Sales. The goal is to improve sales performance, optimize inventory decisions, and support data-driven expansion strategies.

The analysis focuses on identifying high-performing car brands, understanding pricing dynamics, and evaluating regional sales trends using SQL and data visualization tools.

---

## 🎯 Project Objectives

* Identify top-performing car makes and models
* Analyze the relationship between price, condition, and mileage
* Evaluate sales performance across regions
* Understand trends in customer purchasing behavior
* Provide recommendations to improve profitability

---

## 📊 Dataset Description

The dataset contains transactional data for car sales, including:

* **Year** – Manufacturing year
* **Make & Model** – Vehicle brand and type
* **Body Type** – SUV, Sedan, etc.
* **Transmission** – Automatic / Manual
* **State** – Location of sale
* **Condition** – Vehicle condition rating
* **Odometer** – Mileage
* **MMR (Market Value)** – Estimated market price
* **Selling Price** – Final transaction price
* **Sale Date** – Date of transaction

---

## 🛠️ Data Processing Steps

The following steps were performed:

* Loaded dataset into SQL environment

* Cleaned and standardized data

* Converted date column into proper date format

* Removed duplicates and handled missing values

* Created calculated fields:

  * **Revenue** = Selling Price
  * **Price Difference** = Selling Price − MMR
  * **Profit Margin (%)** = (Selling Price − MMR) / Selling Price × 100

* Extracted time-based features:

  * Year
  * Month
  * Day

---

## 🧰 Tools & Technologies Used

* **SQL (BigQuery / Snowflake / MySQL)**
* **Microsoft Excel / Power BI**
* **GitHub**
* **Miro (Data Architecture)**
* **PowerPoint / Canva**

---

## 🧠 Data Analysis

The dataset was analyzed to identify:

* Revenue by car make and model
* Sales distribution by year and body type
* Regional sales performance (state level)
* Pricing trends over time
* Relationship between mileage, condition, and price

---

## 🔍 Key Insights (Example)

* Certain brands (e.g., BMW, Kia, Volvo) dominate sales volume
* Vehicles with lower mileage tend to sell at higher prices
* Market value (MMR) closely influences selling price
* Some regions generate significantly more sales than others
* Newer vehicles generally achieve higher selling prices

---

## 🚀 Recommendations

* Focus inventory on high-performing brands and models
* Prioritize low-mileage vehicles to maximize selling price
* Expand operations in top-performing states
* Optimize pricing strategies based on MMR trends
* Monitor condition and mileage to improve profitability

---

## 🏗️ Project Structure

This repository contains:

* SQL scripts (`car_sales_queries.sql`)
* Processed dataset
* Data visualizations (Excel / Power BI)
* Presentation slides (`BrightMotors_Presentation.pdf`)
* Architecture diagram
* README file

---

## 🧩 Data Architecture

```text
Raw CSV Data
      ↓
Data Cleaning (SQL)
      ↓
Database Storage
      ↓
Analysis (SQL)
      ↓
Visualization (Excel / Power BI)
      ↓
Presentation
```

---

## 👤 Author

**Siphamandla Moyo**

---

## 📥 Submission

This project was completed as part of a BrightLearn Data Analytics case study focused on generating business insights for Bright Motors. 
