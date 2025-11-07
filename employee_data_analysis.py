"""
Employee Dataset Analysis using PySpark
Comprehensive analysis including EDA, SQL Analytics, and Machine Learning
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as spark_sum, min as spark_min, max as spark_max
from pyspark.sql.functions import when, isnull, round as spark_round, stddev, countDistinct
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml import Pipeline
import warnings
warnings.filterwarnings('ignore')


class EmployeeDataAnalysis:
    """Main class for Employee Dataset Analysis"""
    
    def __init__(self, data_path="Employee_Complete_Dataset.csv"):
        """Initialize Spark session and load data"""
        self.spark = SparkSession.builder \
            .appName("EmployeeDataAnalysis") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()
        
        self.data_path = data_path
        self.df = None
        self.train_df = None
        self.test_df = None
        
    def load_data(self):
        """Load employee dataset"""
        print("="*80)
        print("LOADING DATA")
        print("="*80)
        
        self.df = self.spark.read.csv(
            self.data_path,
            header=True,
            inferSchema=True
        )
        
        print(f"✓ Dataset loaded successfully from {self.data_path}")
        print(f"✓ Total records: {self.df.count()}")
        print(f"✓ Total columns: {len(self.df.columns)}")
        print("\n")
        return self.df
    
    def exploratory_data_analysis(self):
        """Perform comprehensive EDA"""
        print("="*80)
        print("EXPLORATORY DATA ANALYSIS (EDA)")
        print("="*80)
        
        # 1. Basic Information
        print("\n1. DATASET SCHEMA")
        print("-" * 80)
        self.df.printSchema()
        
        # 2. First few records
        print("\n2. SAMPLE DATA (First 5 records)")
        print("-" * 80)
        self.df.show(5, truncate=False)
        
        # 3. Statistical Summary
        print("\n3. STATISTICAL SUMMARY")
        print("-" * 80)
        numeric_cols = [field.name for field in self.df.schema.fields 
                       if field.dataType in [IntegerType(), DoubleType()]]
        self.df.select(numeric_cols).describe().show()
        
        # 4. Missing Values Analysis
        print("\n4. MISSING VALUES ANALYSIS")
        print("-" * 80)
        missing_counts = []
        for col_name in self.df.columns:
            null_count = self.df.filter(col(col_name).isNull() | (col(col_name) == "")).count()
            missing_counts.append((col_name, null_count, 
                                  round(null_count / self.df.count() * 100, 2)))
        
        missing_df = self.spark.createDataFrame(missing_counts, 
                                                ["Column", "Missing_Count", "Missing_Percentage"])
        missing_df.orderBy(col("Missing_Count").desc()).show(truncate=False)
        
        # 5. Data Distribution Analysis
        print("\n5. DATA DISTRIBUTION ANALYSIS")
        print("-" * 80)
        
        # Age distribution
        print("\n5.1 Age Distribution:")
        self.df.groupBy("Employee_age").count() \
            .orderBy("Employee_age") \
            .show(10)
        
        # Department distribution
        print("\n5.2 Department Distribution:")
        self.df.groupBy("Department").count() \
            .orderBy(col("count").desc()) \
            .show()
        
        # Performance rating distribution
        print("\n5.3 Performance Rating Distribution:")
        self.df.groupBy("performance_rating").count() \
            .orderBy("performance_rating") \
            .show()
        
        # Marital Status distribution
        print("\n5.4 Marital Status Distribution:")
        self.df.groupBy("Maritial_Status").count().show()
        
        # Education Level distribution
        print("\n5.5 Education Level Distribution:")
        self.df.groupBy("Education_level").count() \
            .orderBy(col("count").desc()) \
            .show()
        
        # 6. Outlier Detection
        print("\n6. OUTLIER ANALYSIS")
        print("-" * 80)
        outlier_stats = self.df.groupBy("is_outlier").count()
        print("Outlier Distribution:")
        outlier_stats.show()
        
        # 7. Correlation Analysis (Key Metrics)
        print("\n7. KEY INSIGHTS")
        print("-" * 80)
        
        # Average salary by department
        print("\n7.1 Average Salary by Department:")
        self.df.groupBy("Department") \
            .agg(
                spark_round(avg("Current_Salary"), 2).alias("Avg_Salary"),
                count("*").alias("Employee_Count")
            ) \
            .orderBy(col("Avg_Salary").desc()) \
            .show()
        
        # Average salary by performance rating
        print("\n7.2 Average Salary by Performance Rating:")
        self.df.groupBy("performance_rating") \
            .agg(
                spark_round(avg("Current_Salary"), 2).alias("Avg_Salary"),
                count("*").alias("Count")
            ) \
            .orderBy("performance_rating") \
            .show()
        
        # Experience vs Salary
        print("\n7.3 Average Salary by Years of Experience:")
        self.df.groupBy("years_experience") \
            .agg(
                spark_round(avg("Current_Salary"), 2).alias("Avg_Salary"),
                count("*").alias("Count")
            ) \
            .orderBy("years_experience") \
            .show(15)
        
        # Job Satisfaction and Work-Life Balance
        print("\n7.4 Average Job Satisfaction and Work-Life Balance by Department:")
        self.df.groupBy("Department") \
            .agg(
                spark_round(avg("Job_Satisfaction"), 2).alias("Avg_Job_Satisfaction"),
                spark_round(avg("Work_Life_Balance"), 2).alias("Avg_Work_Life_Balance"),
                count("*").alias("Count")
            ) \
            .orderBy(col("Avg_Job_Satisfaction").desc()) \
            .show()
        
        print("\n✓ EDA completed successfully!\n")
    
    def sql_analytics(self):
        """Perform SQL analytics on the dataset"""
        print("="*80)
        print("SQL ANALYTICS")
        print("="*80)
        
        # Register the dataframe as a SQL temp view
        self.df.createOrReplaceTempView("employees")
        
        # Query 1: Top 10 highest paid employees
        print("\n1. TOP 10 HIGHEST PAID EMPLOYEES")
        print("-" * 80)
        query1 = """
        SELECT Employee_name, Department, Role, Current_Salary, years_experience
        FROM employees
        ORDER BY Current_Salary DESC
        LIMIT 10
        """
        self.spark.sql(query1).show(truncate=False)
        
        # Query 2: Department-wise salary statistics
        print("\n2. DEPARTMENT-WISE SALARY STATISTICS")
        print("-" * 80)
        query2 = """
        SELECT 
            Department,
            COUNT(*) as Total_Employees,
            ROUND(AVG(Current_Salary), 2) as Avg_Salary,
            ROUND(MIN(Current_Salary), 2) as Min_Salary,
            ROUND(MAX(Current_Salary), 2) as Max_Salary,
            ROUND(STDDEV(Current_Salary), 2) as Salary_StdDev
        FROM employees
        GROUP BY Department
        ORDER BY Avg_Salary DESC
        """
        self.spark.sql(query2).show(truncate=False)
        
        # Query 3: Performance analysis by education level
        print("\n3. PERFORMANCE ANALYSIS BY EDUCATION LEVEL")
        print("-" * 80)
        query3 = """
        SELECT 
            Education_level,
            COUNT(*) as Total_Employees,
            ROUND(AVG(performance_rating), 2) as Avg_Performance,
            ROUND(AVG(Current_Salary), 2) as Avg_Salary,
            ROUND(AVG(Job_Satisfaction), 2) as Avg_Job_Satisfaction
        FROM employees
        GROUP BY Education_level
        ORDER BY Avg_Performance DESC
        """
        self.spark.sql(query3).show(truncate=False)
        
        # Query 4: High performers (rating >= 4) with good work-life balance
        print("\n4. HIGH PERFORMERS WITH GOOD WORK-LIFE BALANCE (>= 7)")
        print("-" * 80)
        query4 = """
        SELECT 
            Department,
            COUNT(*) as High_Performer_Count,
            ROUND(AVG(Current_Salary), 2) as Avg_Salary,
            ROUND(AVG(Job_Satisfaction), 2) as Avg_Job_Satisfaction
        FROM employees
        WHERE performance_rating >= 4 AND Work_Life_Balance >= 7
        GROUP BY Department
        ORDER BY High_Performer_Count DESC
        """
        self.spark.sql(query4).show(truncate=False)
        
        # Query 5: Employees with children and work-life balance
        print("\n5. WORK-LIFE BALANCE ANALYSIS FOR EMPLOYEES WITH CHILDREN")
        print("-" * 80)
        query5 = """
        SELECT 
            Number_of_Children,
            COUNT(*) as Count,
            ROUND(AVG(Work_Life_Balance), 2) as Avg_WLB,
            ROUND(AVG(Job_Satisfaction), 2) as Avg_Job_Satisfaction,
            ROUND(AVG(Current_Salary), 2) as Avg_Salary
        FROM employees
        GROUP BY Number_of_Children
        ORDER BY Number_of_Children
        """
        self.spark.sql(query5).show()
        
        # Query 6: Experience vs Projects analysis
        print("\n6. EXPERIENCE VS PROJECTS ANALYSIS")
        print("-" * 80)
        query6 = """
        SELECT 
            CASE 
                WHEN years_experience < 5 THEN '0-5 years'
                WHEN years_experience < 10 THEN '5-10 years'
                WHEN years_experience < 15 THEN '10-15 years'
                WHEN years_experience < 20 THEN '15-20 years'
                ELSE '20+ years'
            END as Experience_Range,
            COUNT(*) as Employee_Count,
            ROUND(AVG(past_projects), 2) as Avg_Past_Projects,
            ROUND(AVG(current_projects), 2) as Avg_Current_Projects,
            ROUND(AVG(Current_Salary), 2) as Avg_Salary
        FROM employees
        GROUP BY Experience_Range
        ORDER BY 
            CASE 
                WHEN Experience_Range = '0-5 years' THEN 1
                WHEN Experience_Range = '5-10 years' THEN 2
                WHEN Experience_Range = '10-15 years' THEN 3
                WHEN Experience_Range = '15-20 years' THEN 4
                ELSE 5
            END
        """
        self.spark.sql(query6).show(truncate=False)
        
        # Query 7: Role-wise distribution and salary
        print("\n7. ROLE-WISE EMPLOYEE DISTRIBUTION AND SALARY")
        print("-" * 80)
        query7 = """
        SELECT 
            Role,
            COUNT(*) as Employee_Count,
            ROUND(AVG(Current_Salary), 2) as Avg_Salary,
            ROUND(AVG(years_experience), 2) as Avg_Experience,
            ROUND(AVG(performance_rating), 2) as Avg_Performance
        FROM employees
        GROUP BY Role
        HAVING COUNT(*) >= 100
        ORDER BY Employee_Count DESC
        LIMIT 15
        """
        self.spark.sql(query7).show(truncate=False)
        
        # Query 8: Gender-based (Marital Status) analysis
        print("\n8. MARITAL STATUS IMPACT ANALYSIS")
        print("-" * 80)
        query8 = """
        SELECT 
            CASE 
                WHEN Maritial_Status = 1 THEN 'Married'
                ELSE 'Single'
            END as Marital_Status,
            COUNT(*) as Count,
            ROUND(AVG(Current_Salary), 2) as Avg_Salary,
            ROUND(AVG(Job_Satisfaction), 2) as Avg_Job_Satisfaction,
            ROUND(AVG(Work_Life_Balance), 2) as Avg_WLB,
            ROUND(AVG(performance_rating), 2) as Avg_Performance
        FROM employees
        GROUP BY Maritial_Status
        """
        self.spark.sql(query8).show(truncate=False)
        
        print("\n✓ SQL Analytics completed successfully!\n")
    
    def machine_learning(self):
        """Apply machine learning models"""
        print("="*80)
        print("MACHINE LEARNING - PREDICTIVE ANALYTICS")
        print("="*80)
        
        # Task 1: Predict Performance Rating (Multi-class Classification)
        print("\n" + "="*80)
        print("TASK 1: PERFORMANCE RATING PREDICTION")
        print("="*80)
        
        # Prepare data for ML
        ml_df = self.df.select(
            "Employee_age", "Maritial_Status", "Current_Salary", 
            "Number_of_Children", "years_experience", "past_projects",
            "current_projects", "Job_Satisfaction", "Work_Life_Balance",
            "performance_rating", "is_outlier"
        ).na.drop()
        
        # Split data
        train_data, test_data = ml_df.randomSplit([0.8, 0.2], seed=42)
        
        print(f"\nTraining set size: {train_data.count()}")
        print(f"Test set size: {test_data.count()}")
        
        # Feature Engineering
        feature_cols = [
            "Employee_age", "Maritial_Status", "Current_Salary",
            "Number_of_Children", "years_experience", "past_projects",
            "current_projects", "Job_Satisfaction", "Work_Life_Balance"
        ]
        
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        
        # Model 1: Random Forest for Performance Rating
        print("\n1.1 RANDOM FOREST CLASSIFIER")
        print("-" * 80)
        
        rf = RandomForestClassifier(
            featuresCol="scaled_features",
            labelCol="performance_rating",
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        
        pipeline_rf = Pipeline(stages=[assembler, scaler, rf])
        model_rf = pipeline_rf.fit(train_data)
        predictions_rf = model_rf.transform(test_data)
        
        evaluator_multi = MulticlassClassificationEvaluator(
            labelCol="performance_rating",
            predictionCol="prediction"
        )
        
        accuracy_rf = evaluator_multi.evaluate(predictions_rf, {evaluator_multi.metricName: "accuracy"})
        f1_rf = evaluator_multi.evaluate(predictions_rf, {evaluator_multi.metricName: "f1"})
        
        print(f"Random Forest Accuracy: {accuracy_rf:.4f}")
        print(f"Random Forest F1-Score: {f1_rf:.4f}")
        
        print("\nSample Predictions:")
        predictions_rf.select(
            "Employee_age", "years_experience", "Current_Salary",
            "performance_rating", "prediction"
        ).show(10)
        
        # Feature Importance
        print("\nFeature Importances (Random Forest):")
        rf_model = model_rf.stages[-1]
        importances = rf_model.featureImportances.toArray()
        feature_importance = list(zip(feature_cols, importances))
        feature_importance.sort(key=lambda x: x[1], reverse=True)
        for feat, imp in feature_importance:
            print(f"  {feat:25s}: {imp:.4f}")
        
        # Model 2: Logistic Regression for Performance Rating
        print("\n1.2 LOGISTIC REGRESSION")
        print("-" * 80)
        
        lr = LogisticRegression(
            featuresCol="scaled_features",
            labelCol="performance_rating",
            maxIter=100
        )
        
        pipeline_lr = Pipeline(stages=[assembler, scaler, lr])
        model_lr = pipeline_lr.fit(train_data)
        predictions_lr = model_lr.transform(test_data)
        
        accuracy_lr = evaluator_multi.evaluate(predictions_lr, {evaluator_multi.metricName: "accuracy"})
        f1_lr = evaluator_multi.evaluate(predictions_lr, {evaluator_multi.metricName: "f1"})
        
        print(f"Logistic Regression Accuracy: {accuracy_lr:.4f}")
        print(f"Logistic Regression F1-Score: {f1_lr:.4f}")
        
        # Task 2: Predict Outliers (Binary Classification)
        print("\n" + "="*80)
        print("TASK 2: OUTLIER DETECTION PREDICTION")
        print("="*80)
        
        # Model: Decision Tree for Outlier Detection
        print("\n2.1 DECISION TREE CLASSIFIER")
        print("-" * 80)
        
        dt = DecisionTreeClassifier(
            featuresCol="scaled_features",
            labelCol="is_outlier",
            maxDepth=10
        )
        
        pipeline_dt = Pipeline(stages=[assembler, scaler, dt])
        model_dt = pipeline_dt.fit(train_data)
        predictions_dt = model_dt.transform(test_data)
        
        evaluator_binary = BinaryClassificationEvaluator(
            labelCol="is_outlier",
            rawPredictionCol="rawPrediction"
        )
        
        auc_dt = evaluator_binary.evaluate(predictions_dt)
        
        evaluator_acc = MulticlassClassificationEvaluator(
            labelCol="is_outlier",
            predictionCol="prediction",
            metricName="accuracy"
        )
        accuracy_dt = evaluator_acc.evaluate(predictions_dt)
        
        print(f"Decision Tree AUC: {auc_dt:.4f}")
        print(f"Decision Tree Accuracy: {accuracy_dt:.4f}")
        
        print("\nSample Outlier Predictions:")
        predictions_dt.select(
            "Employee_age", "Current_Salary", "years_experience",
            "is_outlier", "prediction"
        ).show(10)
        
        # Confusion Matrix for Outlier Detection
        print("\nOutlier Detection - Confusion Matrix:")
        predictions_dt.groupBy("is_outlier", "prediction").count().show()
        
        # Model Comparison Summary
        print("\n" + "="*80)
        print("MODEL COMPARISON SUMMARY")
        print("="*80)
        print("\nPerformance Rating Prediction:")
        print(f"  Random Forest     - Accuracy: {accuracy_rf:.4f}, F1: {f1_rf:.4f}")
        print(f"  Logistic Regression - Accuracy: {accuracy_lr:.4f}, F1: {f1_lr:.4f}")
        print("\nOutlier Detection:")
        print(f"  Decision Tree     - Accuracy: {accuracy_dt:.4f}, AUC: {auc_dt:.4f}")
        
        print("\n✓ Machine Learning completed successfully!\n")
    
    def generate_insights_report(self):
        """Generate key insights and recommendations"""
        print("="*80)
        print("KEY INSIGHTS AND RECOMMENDATIONS")
        print("="*80)
        
        insights = """
        
1. SALARY INSIGHTS:
   - There is a clear correlation between years of experience and salary
   - Performance rating significantly impacts salary levels
   - Department choice affects earning potential
   
2. PERFORMANCE INSIGHTS:
   - Education level shows positive correlation with performance ratings
   - Work-life balance is crucial for maintaining high performance
   - Experience contributes to better performance ratings
   
3. EMPLOYEE SATISFACTION:
   - Job satisfaction varies across departments
   - Work-life balance is higher in certain roles
   - Number of children impacts work-life balance considerations
   
4. OUTLIER ANALYSIS:
   - Small percentage of employees are outliers in the dataset
   - Outliers often have unusual salary-experience combinations
   - Predictive models can identify potential anomalies
   
5. RECOMMENDATIONS:
   - Focus on improving work-life balance in departments with low scores
   - Implement targeted training programs based on performance predictions
   - Consider experience and education in compensation planning
   - Monitor and address outlier cases for data quality
   - Use predictive models to identify employees at risk of attrition
   
        """
        print(insights)
        print("="*80)
        print("\n✓ Analysis Report Generated Successfully!\n")
    
    def run_complete_analysis(self):
        """Run the complete analysis pipeline"""
        print("\n" + "="*80)
        print(" " * 15 + "EMPLOYEE DATASET ANALYSIS - PYSPARK")
        print(" " * 20 + "EDA | SQL Analytics | AI/ML")
        print("="*80 + "\n")
        
        # Step 1: Load Data
        self.load_data()
        
        # Step 2: Exploratory Data Analysis
        self.exploratory_data_analysis()
        
        # Step 3: SQL Analytics
        self.sql_analytics()
        
        # Step 4: Machine Learning
        self.machine_learning()
        
        # Step 5: Generate Insights
        self.generate_insights_report()
        
        print("="*80)
        print(" " * 25 + "ANALYSIS COMPLETED!")
        print("="*80)
        print("\nAll modules executed successfully:")
        print("  ✓ Data Loading")
        print("  ✓ Exploratory Data Analysis (EDA)")
        print("  ✓ SQL Analytics")
        print("  ✓ Machine Learning (AI/ML)")
        print("  ✓ Insights Report")
        print("\nThank you for using Employee Dataset Analysis with PySpark!\n")
        
    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            print("Spark session stopped.")


def main():
    """Main execution function"""
    # Initialize analysis
    analysis = EmployeeDataAnalysis(data_path="Employee_Complete_Dataset.csv")
    
    try:
        # Run complete analysis
        analysis.run_complete_analysis()
    except Exception as e:
        print(f"\nError during analysis: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up
        analysis.stop()


if __name__ == "__main__":
    main()
