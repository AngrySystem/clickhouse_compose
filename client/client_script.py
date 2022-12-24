from clickhouse_driver import Client
import numpy as np
import pandas as pd
import time

is_ready = False

while not is_ready:
    try:
        client = Client(host='ch_server', port='9000')
        time.sleep(1)
        client.execute('SHOW DATABASES;')

        is_ready = True
        print('Database ready!', end='\n')
    except:
        print('Still not initialized...')


exist = False

databases_ = client.execute("SHOW DATABASES;")
for i in databases_:
    if i[0] == 'logistics':
        exist = True

if not exist:

    seed = 42
    rng = np.random.default_rng(seed)

    num_all = 2_000_000

    client.execute('CREATE DATABASE logistics;')
    client.execute('use logistics;')

    client.execute('CREATE TABLE medicines (med_id UInt32, artikul Int32, nomenclature String, weight UInt32, volume Float64, price Float64) ENGINE = MergeTree() PRIMARY KEY (med_id);')
    client.execute("CREATE TABLE orders (order_id UInt64, med_id UInt32, barcode UInt64, amount UInt32, region_id UInt32, unit String, date DateTime('Asia/Vladivostok')) ENGINE = MergeTree() PRIMARY KEY (order_id);")
    client.execute('CREATE TABLE regions (region_id UInt32, region_name String) ENGINE = MergeTree() PRIMARY KEY (region_id);')


    # Medicines

    # 3 (nomenclature)
    nomenclatures = np.array(pd.read_json("medicine_nomenclatures.txt")[0])

    n_size = len(nomenclatures)

    # 1 (med_id)
    medicine_id = np.arange(n_size)

    # 2 (artikul)
    amount_of_artikuls = int(n_size * 0.25)
    artikuls = rng.choice(4_000_000, size=amount_of_artikuls, replace=False) + 50_000
    res_artikuls = np.zeros((n_size,), dtype=np.int32) # use
    res_artikuls[rng.integers(0, n_size, size=amount_of_artikuls)] = artikuls

    # 4 (weight)
    weight = rng.integers(5, 350, size=n_size, dtype=np.int32)

    # 5 (volume)
    volume = rng.integers(2, 1300, size=n_size) / 10_000

    # 6 (price)
    num_price_70 = int(n_size * 0.7)
    num_price_30 = n_size - num_price_70
    normal_part_price  = np.around(rng.normal(175, 50, size=(num_price_70,)), 2)
    uniform_part_price = np.around(rng.uniform(400, 5000, size=(num_price_30,)), 2)
    price = np.concatenate((normal_part_price, uniform_part_price)) # use
    rng.shuffle(price)

    data_medicines = [[int(medicine_id[i]), int(res_artikuls[i]), nomenclatures[i], int(weight[i]), float(volume[i]), float(price[i])] for i in range(n_size)]


    # Regions

    # 2 (region_name)
    regions = np.array(pd.read_json("regions.txt")[0])

    r_size = len(regions)

    # 1 (region_id)
    ref_id = np.arange(100, 500)
    region_id = rng.choice(ref_id, size=r_size, replace=False) # use
    region_id.sort()

    data_regions = [[region_id[i], regions[i]] for i in range(r_size)]


    # Orders

    # 1 (order_id)
    order_id = np.arange(num_all)

    # 2 (med_id)
    order_medicine_id = rng.integers(0, n_size, size=num_all)

    # 3 (barcode)
    barcode = rng.choice(8_000_000_000, size=num_all, replace=False) + 1_000_000_000

    # 4 (amount)
    num_amount_90 = int(num_all * 0.85)
    num_amount_10 = num_all - num_amount_90
    normal_part_amount  = rng.normal(32, 6, size=(num_amount_90,)).astype(np.int32)
    uniform_part_amount = rng.uniform(60, 1500, size=(num_amount_10)).astype(np.int32)
    amount = np.concatenate((normal_part_amount, uniform_part_amount)) # use
    rng.shuffle(amount)

    # 5 (region_id)
    ref_region_id = rng.integers(0, r_size, size=num_all)
    order_region_id = region_id[ref_region_id] # use

    # 6 (unit)
    unit = np.full((num_all, ), 'шт.')

    # 7 (date)
    initial_time = 1667224800

    normal_part_time  = rng.normal(12, 2, size=num_all-13).astype(np.uint32)
    uniform_part_time = rng.uniform(120, 1800, size=12).astype(np.uint32)
    full_add_time = np.concatenate((normal_part_time, uniform_part_time))
    rng.shuffle(full_add_time)

    full_add_time = np.concatenate(([initial_time], full_add_time))
    dates = np.cumsum(full_add_time) # use

    data_orders = [[order_id[i], order_medicine_id[i], barcode[i], amount[i], order_region_id[i], unit[i], int(dates[i])] for i in range(num_all)]


    client.execute("INSERT INTO logistics.medicines (med_id, artikul, nomenclature, weight, volume, price) VALUES", (i for i in data_medicines))
    client.execute("INSERT INTO logistics.regions (region_id, region_name) VALUES", (i for i in data_regions))
    client.execute("INSERT INTO logistics.orders (order_id, med_id, barcode, amount, region_id, unit, date) VALUES", (i for i in data_orders))


client.execute('use logistics;')

databases = client.execute("SHOW DATABASES;")
tables    = client.execute("SHOW TABLES;")

from_medicines = client.execute("SELECT * FROM medicines limit 5;")
from_regions  = client.execute("SELECT * FROM regions limit 5;")
from_orders   = client.execute("SELECT * FROM orders limit 5;")


def dot_line():
    print('------------------------------')

print(f"Databases: {databases}")
dot_line()
print(f"Tables in logistics database: {tables}")
dot_line()
print("First 5 columns from medicines:")
for i in from_medicines:
    print(i)

dot_line()
print("First 5 columns from regions:")
for i in from_regions:
    print(i)

dot_line()
print("First 5 columns from orders:")
for i in from_orders:
    print(i)

dot_line()

coop = client.execute("""SELECT orders.order_id AS id, med.nomenclature AS name, reg.region_name AS region, orders.date, med.price*orders.amount AS total_sum 
                         FROM orders
                         RIGHT JOIN regions AS reg ON reg.region_id = orders.region_id 
                         RIGHT JOIN medicines AS med ON med.med_id = orders.med_id
                         limit 5""")

print("First 5 columns from coop (order_id, nomenclature, region, date, price*amount):")

for i in coop:
    print(i)