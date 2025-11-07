# Quick Start Guide

## Getting Started with Employee Dataset Analysis

### Prerequisites
Make sure you have:
- Python 3.9 or higher
- Java 8 or higher (required for PySpark)
- pip (Python package manager)

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yadavanujkumar/Cassandra-Employee-Dataset-Exploration-using-Pyspark.git
cd Cassandra-Employee-Dataset-Exploration-using-Pyspark
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

### Quick Start Options

#### Option 1: Run the Demo (Fastest)
Get a quick overview of the project capabilities:
```bash
python demo.py
```
**Output**: Overview of EDA and SQL Analytics (runs in ~30 seconds)

#### Option 2: Run Complete Analysis
Execute the full pipeline including Machine Learning:
```bash
python employee_data_analysis.py
```
**Output**: Complete analysis with EDA, SQL Analytics, and ML models (runs in ~5-10 minutes)

#### Option 3: Interactive Notebook
Explore the data interactively:
```bash
jupyter notebook employee_analysis_notebook.ipynb
```
**Benefits**: Step-by-step execution, visualization, custom queries

### What You'll Get

#### 1. Exploratory Data Analysis (EDA)
- Dataset schema and statistics
- Missing value analysis
- Distribution analysis for all key features
- Correlation insights
- Outlier detection

#### 2. SQL Analytics
- Top performers identification
- Department-wise salary analysis
- Performance by education level
- Work-life balance insights
- Experience vs salary correlation
- Role-wise statistics
- Marital status impact analysis

#### 3. Machine Learning (AI/ML)
- **Performance Rating Prediction**
  - Random Forest Classifier (~85% accuracy)
  - Logistic Regression
  - Feature importance analysis
  
- **Outlier Detection**
  - Decision Tree Classifier (~95% accuracy)
  - Confusion matrix
  - Model comparison

### Sample Output

When you run the demo, you'll see output like:

```
================================================================================
                    EMPLOYEE DATA ANALYSIS - DEMO
                         PySpark Project
================================================================================

📊 Loading Employee Dataset...
✓ Loaded 50,000 employee records with 19 features

================================================================================
1. DATASET OVERVIEW
================================================================================

First 3 records:
+--------------+----------+-------------+--------------+----------------+------------------+
|Employee_name |Department|Role         |Current_Salary|years_experience|performance_rating|
+--------------+----------+-------------+--------------+----------------+------------------+
|Karen Anderson|R&D       |Researcher   |116138        |12              |3                 |
|David Taylor  |HR        |HR Executive |82171         |10              |5                 |
|Nina Kumar    |Sales     |Sales Manager|48600         |0               |4                 |
+--------------+----------+-------------+--------------+----------------+------------------+

Overall Statistics:
• Average Salary: $58,396.14
• Average Experience: 8.3 years
• Average Job Satisfaction: 6.55/10
• Average Work-Life Balance: 4.52/10
```

### Customization

You can easily customize the analysis:

```python
from employee_data_analysis import EmployeeDataAnalysis

# Initialize with your own data
analysis = EmployeeDataAnalysis(data_path="your_data.csv")

# Run specific components
analysis.load_data()
analysis.exploratory_data_analysis()  # Run only EDA
analysis.sql_analytics()              # Run only SQL analytics
analysis.machine_learning()           # Run only ML

# Or run everything
analysis.run_complete_analysis()

# Clean up
analysis.stop()
```

### Custom SQL Queries

```python
# After loading data
analysis.df.createOrReplaceTempView("employees")

# Write your own queries
custom_query = """
    SELECT Department, Role, AVG(Current_Salary) as Avg_Salary
    FROM employees
    WHERE performance_rating >= 4
    GROUP BY Department, Role
    ORDER BY Avg_Salary DESC
    LIMIT 10
"""

results = analysis.spark.sql(custom_query)
results.show()
```

### Troubleshooting

**Issue**: Java not found
```bash
# Install Java 8 or higher
sudo apt-get install openjdk-8-jdk  # Ubuntu/Debian
brew install openjdk@8               # macOS
```

**Issue**: Memory errors
```bash
# Reduce Spark memory in the script
.config("spark.driver.memory", "2g")  # Change from 4g to 2g
```

**Issue**: Dataset not found
```bash
# Make sure you're in the project directory
cd Cassandra-Employee-Dataset-Exploration-using-Pyspark
# Check if CSV files exist
ls -la *.csv
```

### Next Steps

1. **Explore the data**: Run the demo to understand the dataset
2. **Deep dive**: Run the complete analysis for full insights
3. **Experiment**: Use the notebook for interactive exploration
4. **Customize**: Modify queries and models for your needs
5. **Integrate**: Use with Cassandra database (see Employee_Dataset.sql)

### Learning Resources

- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [PySpark ML Guide](https://spark.apache.org/docs/latest/ml-guide.html)
- [SQL Analytics with Spark](https://spark.apache.org/docs/latest/sql-programming-guide.html)

### Support

For issues or questions:
1. Check the main [README.md](README.md)
2. Review the [troubleshooting](#troubleshooting) section
3. Open an issue on GitHub

---

Happy Analyzing! 🎉
