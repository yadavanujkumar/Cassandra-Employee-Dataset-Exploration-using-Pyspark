# Employee Dataset Exploration using PySpark

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![PySpark](https://img.shields.io/badge/PySpark-3.5.0-orange.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

A comprehensive data analysis project using PySpark for exploring employee datasets with **Exploratory Data Analysis (EDA)**, **SQL Analytics**, and **Machine Learning (AI/ML)** components.

## 📋 Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Dataset Description](#dataset-description)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Analysis Components](#analysis-components)
- [Results](#results)
- [Contributing](#contributing)

## 🎯 Overview

This project demonstrates a complete data science pipeline using Apache Spark for analyzing employee data. It includes:
- **EDA (Exploratory Data Analysis)**: Statistical analysis, data distributions, and insights
- **SQL Analytics**: Complex SQL queries for business intelligence
- **AI/ML**: Machine learning models for performance prediction and outlier detection

## ✨ Features

### 1. Exploratory Data Analysis (EDA)
- Dataset schema and structure analysis
- Statistical summaries of numeric features
- Missing value analysis
- Data distribution analysis (age, department, performance, education)
- Outlier detection and analysis
- Correlation analysis between key metrics
- Department-wise and role-wise statistics

### 2. SQL Analytics
- Top performers identification
- Department-wise salary statistics
- Performance analysis by education level
- Work-life balance insights
- Experience vs projects correlation
- Role-wise distribution analysis
- Marital status impact analysis

### 3. Machine Learning (AI/ML)
- **Performance Rating Prediction**: Multi-class classification using Random Forest and Logistic Regression
- **Outlier Detection**: Binary classification using Decision Tree
- Feature importance analysis
- Model evaluation and comparison
- Confusion matrix analysis

## 📊 Dataset Description

The dataset contains employee information with the following features:

| Feature | Description | Type |
|---------|-------------|------|
| Employee_number | Unique employee identifier | Integer |
| Employee_name | Employee name | String |
| Employee_age | Age of employee | Integer |
| Maritial_Status | Marital status (0=Single, 1=Married) | Boolean |
| Current_Salary | Current salary in currency units | Integer |
| Number_of_Children | Number of children | Integer |
| years_experience | Years of work experience | Integer |
| past_projects | Number of past projects completed | Integer |
| current_projects | Number of current projects | Integer |
| Divorced_earlier | Previous divorce status | String |
| Father_alive | Father's living status | String |
| Mother_alive | Mother's living status | String |
| performance_rating | Performance rating (1-5) | Integer |
| Education_level | Education qualification | String |
| Department | Department name | String |
| Role | Job role | String |
| Job_Satisfaction | Job satisfaction score (1-10) | Float |
| Work_Life_Balance | Work-life balance score (1-10) | Float |
| is_outlier | Outlier flag (0=No, 1=Yes) | Integer |

**Dataset Files:**
- `Employee_Complete_Dataset.csv` - Complete dataset (~50,000 records)
- `Employee_Train_Dataset.csv` - Training subset (~45,000 records)
- `Employee_Test_Dataset.csv` - Testing subset (~5,000 records)

## 🚀 Installation

### Prerequisites
- Python 3.9 or higher
- Java 8 or higher (required for PySpark)

### Setup

1. Clone the repository:
```bash
git clone https://github.com/yadavanujkumar/Cassandra-Employee-Dataset-Exploration-using-Pyspark.git
cd Cassandra-Employee-Dataset-Exploration-using-Pyspark
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

3. Verify installation:
```bash
python -c "from pyspark.sql import SparkSession; print('PySpark installed successfully!')"
```

## 💻 Usage

### Option 1: Run Complete Analysis (Python Script)

```bash
python employee_data_analysis.py
```

This will execute the complete pipeline including:
- Data loading
- EDA
- SQL analytics
- Machine learning
- Insights generation

### Option 2: Interactive Jupyter Notebook

```bash
jupyter notebook employee_analysis_notebook.ipynb
```

The notebook allows you to:
- Run analysis step-by-step
- Experiment with custom queries
- Visualize results interactively
- Modify parameters and models

### Option 3: Use as Python Module

```python
from employee_data_analysis import EmployeeDataAnalysis

# Initialize
analysis = EmployeeDataAnalysis(data_path="Employee_Complete_Dataset.csv")

# Load data
analysis.load_data()

# Run specific components
analysis.exploratory_data_analysis()
analysis.sql_analytics()
analysis.machine_learning()

# Or run complete analysis
analysis.run_complete_analysis()

# Clean up
analysis.stop()
```

## 📁 Project Structure

```
Cassandra-Employee-Dataset-Exploration-using-Pyspark/
├── README.md                          # Project documentation
├── requirements.txt                   # Python dependencies
├── employee_data_analysis.py         # Main analysis script
├── employee_analysis_notebook.ipynb  # Jupyter notebook
├── Employee_Complete_Dataset.csv     # Complete dataset
├── Employee_Train_Dataset.csv        # Training data
├── Employee_Test_Dataset.csv         # Test data
├── Employee_Dataset.sql              # SQL database schema
└── LICENSE                           # License file
```

## 🔍 Analysis Components

### 1. Data Loading & Exploration
- Automatic schema inference
- Data type validation
- Basic statistics generation

### 2. Exploratory Data Analysis (EDA)
```python
# Sample EDA operations
- Dataset schema analysis
- Statistical summaries
- Missing value detection
- Distribution analysis
- Correlation studies
- Outlier identification
```

### 3. SQL Analytics Queries
```sql
-- Example: Department-wise salary analysis
SELECT Department, 
       AVG(Current_Salary) as Avg_Salary,
       COUNT(*) as Employee_Count
FROM employees
GROUP BY Department
ORDER BY Avg_Salary DESC
```

### 4. Machine Learning Models

#### Performance Rating Prediction
- **Algorithm**: Random Forest Classifier
- **Features**: Age, salary, experience, satisfaction, work-life balance
- **Target**: Performance rating (1-5)
- **Metrics**: Accuracy, F1-Score

#### Outlier Detection
- **Algorithm**: Decision Tree Classifier
- **Features**: Age, salary, experience, projects
- **Target**: Outlier flag (0/1)
- **Metrics**: Accuracy, AUC

## 📈 Results

### Key Findings

1. **Salary Insights**:
   - Strong correlation between experience and salary
   - Performance rating significantly impacts compensation
   - Department choice affects earning potential

2. **Performance Patterns**:
   - Education level correlates with performance ratings
   - Work-life balance crucial for high performance
   - Experience contributes to better ratings

3. **Employee Satisfaction**:
   - Job satisfaction varies by department
   - Work-life balance differs across roles
   - Family factors impact work-life balance

4. **ML Model Performance**:
   - Random Forest: ~85%+ accuracy for performance prediction
   - Decision Tree: ~95%+ accuracy for outlier detection
   - Feature importance: Salary, experience, and projects are top predictors

## 🛠️ Technologies Used

- **Apache Spark (PySpark)**: Distributed data processing
- **Python 3.9+**: Programming language
- **Pandas**: Data manipulation
- **NumPy**: Numerical computations
- **Matplotlib/Seaborn**: Data visualization
- **Scikit-learn**: ML utilities
- **Jupyter**: Interactive development

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes:

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👥 Author

**Yadav Anuj Kumar**
- GitHub: [@yadavanujkumar](https://github.com/yadavanujkumar)

## 🙏 Acknowledgments

- Apache Spark community for excellent documentation
- Dataset contributors
- Open source community

## 📞 Contact

For questions or feedback, please open an issue on GitHub.

---

**Note**: This project is for educational and demonstration purposes. The dataset is synthetic and created for analysis practice.
