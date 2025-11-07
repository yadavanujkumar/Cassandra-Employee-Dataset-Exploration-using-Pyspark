"""
Demo script to showcase the Employee Data Analysis with sample outputs
This script runs a subset of the analysis to demonstrate capabilities
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round as spark_round
import warnings
warnings.filterwarnings('ignore')

def run_demo():
    """Run a quick demo of the analysis capabilities"""
    
    print("\n" + "="*80)
    print(" " * 20 + "EMPLOYEE DATA ANALYSIS - DEMO")
    print(" " * 25 + "PySpark Project")
    print("="*80 + "\n")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("EmployeeAnalysisDemo") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    # Load data
    print("📊 Loading Employee Dataset...")
    df = spark.read.csv("Employee_Complete_Dataset.csv", header=True, inferSchema=True)
    print(f"✓ Loaded {df.count():,} employee records with {len(df.columns)} features\n")
    
    # Demo 1: Basic Statistics
    print("="*80)
    print("1. DATASET OVERVIEW")
    print("="*80)
    print("\nFirst 3 records:")
    df.select("Employee_name", "Department", "Role", "Current_Salary", 
              "years_experience", "performance_rating").show(3, truncate=False)
    
    # Demo 2: Department Analysis
    print("\n" + "="*80)
    print("2. DEPARTMENT-WISE ANALYSIS")
    print("="*80)
    df.groupBy("Department") \
        .agg(
            count("*").alias("Employees"),
            spark_round(avg("Current_Salary"), 2).alias("Avg_Salary"),
            spark_round(avg("Job_Satisfaction"), 2).alias("Avg_Satisfaction")
        ) \
        .orderBy(col("Avg_Salary").desc()) \
        .show(truncate=False)
    
    # Demo 3: SQL Query Example
    print("\n" + "="*80)
    print("3. SQL ANALYTICS EXAMPLE")
    print("="*80)
    df.createOrReplaceTempView("employees")
    
    query = """
    SELECT 
        Education_level,
        COUNT(*) as Count,
        ROUND(AVG(Current_Salary), 2) as Avg_Salary,
        ROUND(AVG(performance_rating), 2) as Avg_Performance
    FROM employees
    GROUP BY Education_level
    ORDER BY Avg_Performance DESC
    """
    
    print("\nPerformance by Education Level:")
    spark.sql(query).show(truncate=False)
    
    # Demo 4: High Performers
    print("\n" + "="*80)
    print("4. HIGH PERFORMERS (Rating >= 4)")
    print("="*80)
    high_performers = df.filter(col("performance_rating") >= 4)
    print(f"\nTotal High Performers: {high_performers.count():,} "
          f"({high_performers.count() / df.count() * 100:.1f}% of workforce)\n")
    
    high_performers.groupBy("Department") \
        .agg(
            count("*").alias("High_Performers"),
            spark_round(avg("Current_Salary"), 2).alias("Avg_Salary")
        ) \
        .orderBy(col("High_Performers").desc()) \
        .show(truncate=False)
    
    # Demo 5: Insights Summary
    print("\n" + "="*80)
    print("5. KEY INSIGHTS")
    print("="*80)
    
    insights = df.agg(
        spark_round(avg("Current_Salary"), 2).alias("Avg_Salary"),
        spark_round(avg("years_experience"), 2).alias("Avg_Experience"),
        spark_round(avg("Job_Satisfaction"), 2).alias("Avg_Satisfaction"),
        spark_round(avg("Work_Life_Balance"), 2).alias("Avg_WLB")
    ).collect()[0]
    
    print(f"""
    Overall Statistics:
    • Average Salary: ${insights['Avg_Salary']:,.2f}
    • Average Experience: {insights['Avg_Experience']:.1f} years
    • Average Job Satisfaction: {insights['Avg_Satisfaction']:.2f}/10
    • Average Work-Life Balance: {insights['Avg_WLB']:.2f}/10
    """)
    
    # Demo 6: Feature Correlations
    print("\n" + "="*80)
    print("6. EXPERIENCE VS SALARY ANALYSIS")
    print("="*80)
    
    df.createOrReplaceTempView("emp")
    exp_salary = spark.sql("""
        SELECT 
            CASE 
                WHEN years_experience < 5 THEN '0-5 years'
                WHEN years_experience < 10 THEN '5-10 years'
                WHEN years_experience < 15 THEN '10-15 years'
                WHEN years_experience < 20 THEN '15-20 years'
                ELSE '20+ years'
            END as Experience_Range,
            ROUND(AVG(Current_Salary), 2) as Avg_Salary
        FROM emp
        GROUP BY Experience_Range
        ORDER BY 
            CASE 
                WHEN Experience_Range = '0-5 years' THEN 1
                WHEN Experience_Range = '5-10 years' THEN 2
                WHEN Experience_Range = '10-15 years' THEN 3
                WHEN Experience_Range = '15-20 years' THEN 4
                ELSE 5
            END
    """)
    exp_salary.show(truncate=False)
    
    print("\n" + "="*80)
    print("DEMO COMPLETED SUCCESSFULLY!")
    print("="*80)
    print("""
    This demo showcased:
    ✓ Data loading and exploration
    ✓ Statistical analysis
    ✓ SQL queries on employee data
    ✓ Department and role analysis
    ✓ Performance insights
    
    For complete analysis including Machine Learning, run:
        python employee_data_analysis.py
    
    Or use the interactive notebook:
        jupyter notebook employee_analysis_notebook.ipynb
    """)
    
    spark.stop()

if __name__ == "__main__":
    run_demo()
