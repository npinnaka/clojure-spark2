val basedf = spark.read.json("resources/cartypes.json")
val explodeddf = basedf.select($"id",explode($"cars").as("d"))
val whendf = explodeddf.select($"id", when($"d.department"=== "IT", $"d.department").as("IT_department"), when($"d.department"==="IT", $"d.emp_count").as("IT_emp_count"),when( $"d.department"==="IT",  $"d.expenses").as("IT_expenses"), when($"d.department"=== "sales", $"d.department").as("sales_department"), when($"d.department"==="sales", $"d.emp_count").as("sales_emp_count"),when( $"d.department"==="sales",  $"d.expenses").as("sales_expenses"), when($"d.department"=== "marketing", $"d.department").as("marketing_department"), when($"d.department"==="marketing", $"d.emp_count").as("marketing_emp_count"),when( $"d.department"==="marketing",  $"d.expenses").as("marketing_expenses"), when($"d.department"=== "operations", $"d.department").as("operations_department"), when($"d.department"==="operations", $"d.emp_count").as("operations_emp_count"),when( $"d.department"==="operations",  $"d.expenses").as("operations_expenses"), when($"d.department"=== "finance", $"d.department").as("finance_department"), when($"d.department"==="finance", $"d.emp_count").as("finance_emp_count"),when( $"d.department"==="finance",  $"d.expenses").as("finance_expenses"), when($"d.department"=== "HR", $"d.department").as("HR_department"), when($"d.department"==="HR", $"d.emp_count").as("HR_emp_count"),when( $"d.department"==="HR",  $"d.expenses").as("HR_expenses") )
val finaldf  = whendf.select($"id",$"IT_department",$"IT_emp_count",$"IT_expenses",$"sales_department", $"sales_emp_count", $"sales_expenses", $"marketing_department",$"marketing_emp_count",$"marketing_expenses",$"operations_department", $"operations_emp_count",$"operations_expenses",$"finance_department",$"finance_emp_count",$"finance_expenses", $"HR_department",$"HR_emp_count",$"HR_expenses").groupBy("id").agg(max("IT_department").as("IT_department"), max("IT_emp_count").as("IT_emp_count"),max("IT_expenses").as("IT_expenses"), max("sales_department").as("sales_department"), max("sales_emp_count").as("sales_emp_count"),max("sales_expenses").as("sales_expenses"), max("marketing_department").as("marketing_department"), max("marketing_emp_count").as("marketing_emp_count"),max("marketing_expenses").as("marketing_expenses"),max("operations_department").as("operations_department"), max("operations_emp_count").as("operations_emp_count"),max("operations_expenses").as("operations_expenses"), max("finance_department").as("finance_department"),max("finance_emp_count").as("finance_emp_count"),max("finance_expenses").as("finance_expenses"),max("HR_department").as("HR_department"), max("HR_emp_count").as("HR_emp_count"),max("HR_expenses").as("HR_expenses"))