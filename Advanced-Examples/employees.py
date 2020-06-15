from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[*]").setAppName("employees")
sc = SparkContext(conf = conf)

employees = [['Raffery',31], ['Jones',33], ['Heisenberg',33], ['Robinson',34], ['Smith',34]]
department = [31,33]

employees = sc.parallelize(employees)
department = sc.parallelize(department)

dept = department.collect()

employeesWithValidDeptRdd = employees.filter(lambda e: e[1] in dept).collect()

print(employeesWithValidDeptRdd)
