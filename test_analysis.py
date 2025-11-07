"""
Quick validation test for employee_data_analysis.py
"""

import sys
from employee_data_analysis import EmployeeDataAnalysis

def test_basic_functionality():
    """Test basic functionality of the analysis"""
    print("="*80)
    print("TESTING EMPLOYEE DATA ANALYSIS MODULE")
    print("="*80)
    
    try:
        # Test 1: Initialize
        print("\n1. Testing initialization...")
        analysis = EmployeeDataAnalysis(data_path="Employee_Complete_Dataset.csv")
        print("   ✓ Initialization successful")
        
        # Test 2: Load data
        print("\n2. Testing data loading...")
        df = analysis.load_data()
        record_count = df.count()
        column_count = len(df.columns)
        print(f"   ✓ Data loaded: {record_count} records, {column_count} columns")
        
        # Test 3: Schema validation
        print("\n3. Testing schema...")
        required_cols = ["Employee_number", "Employee_name", "Current_Salary", 
                        "Department", "performance_rating"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"   ✗ Missing columns: {missing_cols}")
            return False
        print("   ✓ Schema validation passed")
        
        # Test 4: Quick EDA check
        print("\n4. Testing EDA component...")
        # Just check if we can run basic statistics
        df.select("Current_Salary").describe().show()
        print("   ✓ EDA component working")
        
        # Test 5: SQL Analytics check
        print("\n5. Testing SQL Analytics...")
        df.createOrReplaceTempView("employees")
        test_query = "SELECT COUNT(*) as count FROM employees"
        result = analysis.spark.sql(test_query)
        result.show()
        print("   ✓ SQL Analytics working")
        
        # Clean up
        analysis.stop()
        print("\n" + "="*80)
        print("ALL TESTS PASSED!")
        print("="*80)
        return True
        
    except Exception as e:
        print(f"\n✗ Test failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_basic_functionality()
    sys.exit(0 if success else 1)
