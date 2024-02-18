# Question 1 = (C) 8.382332347441762
# Question 2 = (B) 3.605551275463989
# Question 3 = (A) 353
# Question 4 = (B) 266

# 1. USE A GENERATOR

def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 13
generator = square_root_generator(limit)

sum_limit_5 = 0
i = 0

for sqrt_value in generator:
    sum_limit_5 += sqrt_value
    i += 1

    if i == 5:
      print(f'sum_limit_5 = {sum_limit_5}')
    elif i == 13:
      print(f'13th number yielded = {sqrt_value}')
    print(sqrt_value)

# 2. APPEND A GENERATOR TO A TABLE WITH EXISTING DATA
# 2.1. Load the first generator and calculate the sum of ages of all people. 
#      Make sure to only load it once.

# import dlt
# import duckdb
# import itertools
# pip install dlt[duckdb]
def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}

# define pipeline to duckdb
gen_pipeline = dlt.pipeline(destination='duckdb', dataset_name='people_generator')

# load the generator data to a table in duckdb
info_people_1 = gen_pipeline.run(people_1(), table_name='people', write_disposition='replace')

# create connection to duckdb
conn = duckdb.connect(f'{gen_pipeline.pipeline_name}.duckdb')
conn.sql(f"SET search_path = '{gen_pipeline.dataset_name}'")

# 2.2 Append the second generator to the same table as the first.
# 2.3 After correctly appending the data, calculate the sum of all ages of people.

info_people_2 = gen_pipeline.run(people_2(), table_name='people', write_disposition='append')

# Calcualate sum or all ages of all people loaded
sum_all_ages = conn.sql(f"SELECT SUM(Age) FROM people")
display(sum_all_ages)

gen_1 = people_1()
gen_2 = people_2()
all_persons = itertools.chain(gen_2, gen_1)

# running with merge write disposition
info_merge = gen_pipeline.run(all_persons, table_name='merged_people', write_disposition='merge', primary_key='ID')

all_merged_people = conn.sql(f"SELECT * FROM merged_people")
display(all_merged_people)

sum_all_merged_ages = conn.sql(f"SELECT SUM(Age) FROM merged_people")
display(sum_all_merged_ages)

