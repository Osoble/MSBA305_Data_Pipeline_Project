# Olist E-commerce Data Analysis

This repository contains a comprehensive analysis of the Olist Brazilian E-commerce Public Dataset. The project covers data cleaning, feature engineering, exploratory data analysis, and integration with both SQLite and PostgreSQL databases.

## Table of Contents

- [Project Overview](#project-overview)
- [Dataset](#dataset)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
  - [Local Environment](#local-environment)
  - [Database Setup](#database-setup)
- [Usage](#usage)
- [Analysis Highlights](#analysis-highlights)
- [AI Usage](#ai-usage)
- [Contributing](#contributing)
- [License](#license)

## Project Overview

The primary goal of this project is to explore the Olist e-commerce dataset to uncover insights into sales, customer behavior, product performance, and logistical efficiency. It demonstrates a full data analysis workflow from raw data ingestion to database integration and analytical querying.

## Dataset

The dataset is comprised of various CSV files detailing customer, order, product, seller, payment, and review information from Olist, a Brazilian e-commerce platform. Additionally, external data sources like country GDP and public holidays are integrated for richer analysis. Some sample data is included, but the full dataset is expected to be mounted or downloaded.

## Project Structure

```
. \
├── notebooks/             # Jupyter notebooks for analysis
│   └── olist_analysis.ipynb # Main analysis notebook
├── data/                  # Raw and processed data files (or instructions to get them)
│   ├── olist_customers_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── ...
│   └── publicHolidays.csv
│   └── countries.json
├── scripts/               # Helper scripts (e.g., data ingestion, ETL)
├── olist_cleaned_data.db  # SQLite database file (generated)
├── requirements.txt       # Python dependencies
└── README.md              # Project overview and setup instructions (this file)
```

## Setup Instructions

### Local Environment

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/olist-ecommerce-analysis.git
    cd olist-ecommerce-analysis
    ```

2.  **Create a virtual environment (recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: .\venv\Scripts\activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Download/Mount Data:**
    The notebooks assume the raw CSV files are located in a `data/` directory (or mounted via Google Drive if using Colab, as in the provided notebooks). You will need to obtain the full Olist dataset and place it in the `data/` folder or adjust the `my_path` variable in the notebooks.

### Database Setup

**SQLite:**

The `olist_analysis.ipynb` notebook will generate `olist_cleaned_data.db` locally. No additional setup is typically required beyond running the relevant notebook cells.

**PostgreSQL (Supabase):**

This project also demonstrates integration with a PostgreSQL database (e.g., via Supabase). To use this functionality:

1.  **Set up a Supabase project** and obtain your database connection URL.
2.  **Update the `SUPABASE_DB_URL`** variable in the notebook (or via environment variables) with your connection string:
    ```python
    SUPABASE_DB_URL = "postgresql+psycopg2://[YOUR_USER]:[YOUR_PASSWORD]@[YOUR_HOST]:[YOUR_PORT]/[YOUR_DATABASE]"
    ```
3.  **Data Ingestion:** Please note that during the development of this notebook, an issue was encountered where data insertion into PostgreSQL was interrupted, leading to empty tables. To ensure the PostgreSQL part of the analysis works, you must **successfully re-run the data insertion cells** for all tables (geolocation, product_categories, products, sellers, customers, orders, order_items, order_payments, order_reviews, countries_gdp, holidays).

## Usage

To run the analysis:

1.  Start Jupyter Lab or Jupyter Notebook:
    ```bash
    jupyter lab  # or jupyter notebook
    ```
2.  Open `notebooks/olist_analysis.ipynb` and execute the cells sequentially.

## Analysis Highlights

-   **Data Quality Report:** Detailed summary of missing values, duplicates, and data type conversions.
-   **Feature Engineering:** Creation of new features like `delivery_days`, `delivery_delay`, `total_order_value`, and time-based dimensions.
-   **Key Metrics:** Quarterly revenue trends, delivery performance (on-time rate), average order values by payment type and category.
-   **Geospatial Analysis:** Top states and cities by order volume and revenue.
-   **Seller Performance:** Active sellers over time, correlation between review scores and sales.
-   **Holiday Impact:** Analysis of order volume and delivery times around public holidays.
-   **GDP Correlation:** Examination of Olist's growth in the context of Brazil's GDP.

## AI Usage

This project utilized AI assistance for generating explanations, refining code, and structuring documentation. Prompts used and their corresponding outputs are integrated into the notebook where applicable.

## Contributing

Feel free to fork this repository, open issues, or submit pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
"""
