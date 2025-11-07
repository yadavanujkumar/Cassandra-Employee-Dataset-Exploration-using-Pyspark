"""Quick verification that all project components are working"""
import sys

print("=" * 80)
print("PROJECT VERIFICATION")
print("=" * 80)

checks = []

# Check 1: Imports
print("\n1. Checking imports...")
try:
    from employee_data_analysis import EmployeeDataAnalysis
    checks.append(("Import main module", True))
    print("   ✓ Main module imports successfully")
except Exception as e:
    checks.append(("Import main module", False))
    print(f"   ✗ Import failed: {e}")

# Check 2: Dataset
print("\n2. Checking dataset...")
try:
    import os
    csv_exists = os.path.exists("Employee_Complete_Dataset.csv")
    checks.append(("Dataset exists", csv_exists))
    if csv_exists:
        print("   ✓ Dataset file found")
    else:
        print("   ✗ Dataset file not found")
except Exception as e:
    checks.append(("Dataset exists", False))
    print(f"   ✗ Check failed: {e}")

# Check 3: Documentation
print("\n3. Checking documentation...")
try:
    docs = ["README.md", "QUICKSTART.md", "PROJECT_SUMMARY.md"]
    doc_checks = all(os.path.exists(doc) for doc in docs)
    checks.append(("Documentation complete", doc_checks))
    if doc_checks:
        print("   ✓ All documentation files present")
    else:
        print("   ✗ Some documentation missing")
except Exception as e:
    checks.append(("Documentation complete", False))
    print(f"   ✗ Check failed: {e}")

# Check 4: Scripts
print("\n4. Checking scripts...")
try:
    scripts = ["employee_data_analysis.py", "demo.py", "test_analysis.py", 
               "employee_analysis_notebook.ipynb"]
    script_checks = all(os.path.exists(script) for script in scripts)
    checks.append(("All scripts present", script_checks))
    if script_checks:
        print("   ✓ All script files present")
    else:
        print("   ✗ Some scripts missing")
except Exception as e:
    checks.append(("All scripts present", False))
    print(f"   ✗ Check failed: {e}")

# Summary
print("\n" + "=" * 80)
print("VERIFICATION SUMMARY")
print("=" * 80)
passed = sum(1 for _, result in checks if result)
total = len(checks)
print(f"\nPassed: {passed}/{total} checks")

for check_name, result in checks:
    status = "✓" if result else "✗"
    print(f"  {status} {check_name}")

if passed == total:
    print("\n🎉 All checks passed! Project is ready to use.")
    sys.exit(0)
else:
    print("\n⚠️  Some checks failed. Please review.")
    sys.exit(1)
