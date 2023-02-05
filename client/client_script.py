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

    num_all = 1000 # Кол-во действий

    client.execute('CREATE DATABASE logistics;')
    client.execute('use logistics;')

    client.execute("CREATE TABLE orders (order_id UInt64, region_id Int32, date DateTime('Asia/Vladivostok'), status Int32) ENGINE = MergeTree() PRIMARY KEY (order_id);")
    client.execute('CREATE TABLE regions (region_id Int32, region_name String) ENGINE = MergeTree() PRIMARY KEY (region_id);')
    client.execute("CREATE TABLE batch (batch_id UInt32, batch_barcode UInt64, order_id UInt32) ENGINE = MergeTree() PRIMARY KEY (batch_id);")
    client.execute("CREATE TABLE batch_instance (batch_id UInt32, medicine_barcode UInt64, med_id UInt32) ENGINE = MergeTree() PRIMARY KEY (medicine_barcode);")
    client.execute('CREATE TABLE medicines (med_id UInt32, nomenclature String, weight UInt32, volume Float64, price Float64) ENGINE = MergeTree() PRIMARY KEY (med_id);')


    # Medicines

    # 2 (nomenclature)
    nomenclatures = np.array(pd.read_json("medicine_nomenclatures.txt")[0])

    n_size = len(nomenclatures)

    # 1 (med_id)
    medicine_id = np.arange(n_size)

    # 3 (weight)
    weight = rng.integers(5, 350, size=n_size, dtype=np.int32)

    # 4 (volume)
    volume = rng.integers(10, 650, size=n_size)

    # 5 (price)
    num_price_70 = int(n_size * 0.7)
    num_price_30 = n_size - num_price_70
    normal_part_price  = np.around(rng.normal(175, 50, size=(num_price_70,)), 2)
    uniform_part_price = np.around(rng.uniform(400, 5000, size=(num_price_30,)), 2)
    price = np.concatenate((normal_part_price, uniform_part_price)) # use
    rng.shuffle(price)

    data_medicines = [[int(medicine_id[i]), nomenclatures[i], int(weight[i]), float(volume[i]), float(price[i])] for i in range(n_size)]
    client.execute("INSERT INTO medicines (med_id, nomenclature, weight, volume, price) VALUES", (i for i in data_medicines))


    # Regions

    # 2 (region_name)
    regions = np.array(pd.read_json("regions.txt")[0])

    r_size = len(regions)

    # 1 (region_id)
    pre_region_id = np.arange(100, 500)
    region_id = rng.choice(pre_region_id, size=r_size, replace=False) # use
    region_id.sort()

    data_regions = [[region_id[i], regions[i]] for i in range(r_size)]
    client.execute("INSERT INTO regions (region_id, region_name) VALUES", (i for i in data_regions))


    # Orders

    # 1 (order_id)
    order_id = np.arange(num_all)

    # 4 (status)
    status = rng.integers(0, 2, size=num_all) # 1 - приход, 0 - уход

    # 2 (region_id)
    ref_region_id = rng.integers(0, r_size, size=num_all)
    order_region_id = region_id[ref_region_id] # use
    order_region_id[status.astype(bool)] = -1 

    # 3 (date)
    initial_time = 1667224800

    normal_part_time  = rng.normal(12, 2, size=num_all-13).astype(np.uint32)
    uniform_part_time = rng.uniform(120, 1800, size=12).astype(np.uint32)
    full_add_time = np.concatenate((normal_part_time, uniform_part_time))
    rng.shuffle(full_add_time)

    full_add_time = np.concatenate(([initial_time], full_add_time))
    dates = np.cumsum(full_add_time) # use

    data_orders = [[order_id[i], order_region_id[i], int(dates[i]), status[i]] for i in range(num_all)]
    # client.execute("INSERT INTO orders (order_id, region_id, date, status) VALUES", (i for i in data_orders))


    # Batch

    # 3 (order_id)
    pre_batch_order_id = rng.integers(1, 6, size=num_all)
    batch_order_id = [] # use
    for i in order_id:
        for j in range(pre_batch_order_id[i]):
            batch_order_id.append(i)

    all_batches_len = len(batch_order_id)

    # 1 (batch_id)
    batch_id = np.arange(all_batches_len)

    # 2 (batch_barcode)
    batch_barcode = rng.choice(8_000_000_000, size=all_batches_len, replace=False) + 1_000_000_000

    data_batch = [[batch_id[i], batch_barcode[i], batch_order_id[i]] for i in range(all_batches_len)]
    res_data_batch = [] # Batch

    batch_counter = 0
    for i in pre_batch_order_id:
        tmp = []
        for j in range(i):
            tmp.append(data_batch[batch_counter])
            batch_counter += 1
        res_data_batch.append(tmp)

    # client.execute("INSERT INTO regions (batch_id, batch_barcode, order_id) VALUES", (i for i in data_batch))
    

    # Batch_instance

    # 1 (batch_id)
    amount_in_batch = rng.normal(60, 8, size=all_batches_len).astype(np.uint32) # случайное количество пачек лекарства в коробке

    batch_id_instance = [] # use
    for i, v in enumerate(amount_in_batch):
        for j in range(v):
            batch_id_instance.append(batch_id[i])

    # 2 (medicine_barcode)
    batch_instance_barcode = rng.choice(8_000_000_000, size=len(batch_id_instance), replace=False) + 1_000_000_000

    # 3 (med_id)
    batch_instance_meds_id = [] # use

    counter = 0
    for i in range(num_all):
        rand_med = rng.choice(medicine_id, size=1)[0]
        for j in range(pre_batch_order_id[i]):
            for l in range(amount_in_batch[counter]):
                batch_instance_meds_id.append(rand_med)
            counter += 1

    data_batch_instance = [[batch_id_instance[i], batch_instance_barcode[i], batch_instance_meds_id[i]] for i in range(len(batch_id_instance))]
    res_data_batch_instance = [] # Batch_instance

    instance_counter = 0
    instance_counter_ = 0
    for i in range(num_all):
        tmp = []
        for j in range(pre_batch_order_id[i]):
            for l in range(amount_in_batch[instance_counter]):
                tmp.append(data_batch_instance[instance_counter_])
                instance_counter_ += 1
            instance_counter += 1
        res_data_batch_instance.append(tmp)


    delay_orders_1 = []
    delay_orders_2 = []
    delay_orders_3 = []
    
    for i in range(num_all):

        # Если приход, закидываем на склад
        if data_orders[i][3] == 1:
            client.execute("INSERT INTO orders (order_id, region_id, date, status) VALUES", [data_orders[i]])
            client.execute("INSERT INTO batch (batch_id, batch_barcode, order_id) VALUES", (i for i in res_data_batch[i]))
            client.execute("INSERT INTO batch_instance (batch_id, medicine_barcode, med_id) VALUES", (i for i in res_data_batch_instance[i]))
        # Если уход
        else:
            # cur_order_id = data_orders[i][0]
            cur_med_id = res_data_batch_instance[i][0][2]

            # Кол-во данного товара на складе
            num_now = client.execute(f"""
                                        SELECT count(batch_instance.med_id) 
                                        FROM orders
                                        RIGHT JOIN batch ON orders.order_id = batch.order_id
                                        RIGHT JOIN batch_instance ON batch_instance.batch_id = batch.batch_id
                                        WHERE batch_instance.med_id = {cur_med_id} AND orders.status = 1
                                    """)[0][0]

            # Кол-во требуемое в текущем заказе
            num_now_minus = len(res_data_batch_instance[i])

            if num_now - num_now_minus < 0:
                delay_orders_1.append(data_orders[i])
                delay_orders_2.append(res_data_batch[i])
                delay_orders_3.append(res_data_batch_instance[i])

        print(delay_orders_1)
        # Смотрим сколько данного товара осталось на складе 
        # num_left_storehouse = client.execute(f"SELECT med_good_{data_orders[i][1]} FROM logistics.storehouse")[0][0]




else:
    print('The database to be created already exists!\n')





#     client.execute('CREATE DATABASE logistics;')
#     client.execute('use logistics;')

#     client.execute('CREATE TABLE medicines (med_id UInt32, artikul Int32, nomenclature String, weight UInt32, volume Float64, price Float64) ENGINE = MergeTree() PRIMARY KEY (med_id);')
#     client.execute("CREATE TABLE orders (order_id UInt64, med_id UInt32, barcode UInt64, amount UInt32, region_id UInt32, unit String, date DateTime('Asia/Vladivostok')) ENGINE = MergeTree() PRIMARY KEY (order_id);")
#     client.execute('CREATE TABLE regions (region_id UInt32, region_name String) ENGINE = MergeTree() PRIMARY KEY (region_id);')


#     # Medicines

#     # 3 (nomenclature)
#     nomenclatures = np.array(pd.read_json("medicine_nomenclatures.txt")[0])

#     n_size = len(nomenclatures)

#     # 1 (med_id)
#     medicine_id = np.arange(n_size)

#     # 2 (artikul)
#     amount_of_artikuls = int(n_size * 0.25)
#     artikuls = rng.choice(4_000_000, size=amount_of_artikuls, replace=False) + 50_000
#     res_artikuls = np.zeros((n_size,), dtype=np.int32) # use
#     res_artikuls[rng.integers(0, n_size, size=amount_of_artikuls)] = artikuls

#     # 4 (weight)
#     weight = rng.integers(5, 350, size=n_size, dtype=np.int32)

#     # 5 (volume)
#     volume = rng.integers(2, 1300, size=n_size) / 10_000

#     # 6 (price)
#     num_price_70 = int(n_size * 0.7)
#     num_price_30 = n_size - num_price_70
#     normal_part_price  = np.around(rng.normal(175, 50, size=(num_price_70,)), 2)
#     uniform_part_price = np.around(rng.uniform(400, 5000, size=(num_price_30,)), 2)
#     price = np.concatenate((normal_part_price, uniform_part_price)) # use
#     rng.shuffle(price)

#     data_medicines = [[int(medicine_id[i]), int(res_artikuls[i]), nomenclatures[i], int(weight[i]), float(volume[i]), float(price[i])] for i in range(n_size)]


#     # Regions

#     # 2 (region_name)
#     regions = np.array(pd.read_json("regions.txt")[0])

#     r_size = len(regions)

#     # 1 (region_id)
#     ref_id = np.arange(100, 500)
#     region_id = rng.choice(ref_id, size=r_size, replace=False) # use
#     region_id.sort()

#     data_regions = [[region_id[i], regions[i]] for i in range(r_size)]


#     # Orders

#     # 1 (order_id)
#     order_id = np.arange(num_all)

#     # 2 (med_id)
#     order_medicine_id = rng.integers(0, n_size, size=num_all)

#     # 3 (barcode)
#     # barcode = rng.choice(8_000_000_000, size=num_all, replace=False) + 1_000_000_000
#     num_batch = 

#     # 4 (amount)
#     num_amount_90 = int(num_all * 0.85)
#     num_amount_10 = num_all - num_amount_90
#     normal_part_amount  = rng.normal(32, 6, size=(num_amount_90,)).astype(np.int32) # кол-во покупаемое
#     uniform_part_amount = rng.uniform(60, 1500, size=(num_amount_10)).astype(np.int32)
#     amount = np.concatenate((normal_part_amount, uniform_part_amount)) # use
#     rng.shuffle(amount)

#     # 5 (region_id)
#     ref_region_id = rng.integers(0, r_size, size=num_all)
#     order_region_id = region_id[ref_region_id] # use

#     # 6 (unit)
#     unit = np.full((num_all, ), 'шт.')

#     # 7 (date)
#     initial_time = 1667224800

#     normal_part_time  = rng.normal(12, 2, size=num_all-13).astype(np.uint32)
#     uniform_part_time = rng.uniform(120, 1800, size=12).astype(np.uint32)
#     full_add_time = np.concatenate((normal_part_time, uniform_part_time))
#     rng.shuffle(full_add_time)

#     full_add_time = np.concatenate(([initial_time], full_add_time))
#     dates = np.cumsum(full_add_time) # use

#     # 8 (status)
    

#     # Batch

#     # 1 (barcode)
#     barcode = rng.choice(8_000_000_000, size=num_all, replace=False) + 1_000_000_000

#     data_orders = [[order_id[i], order_medicine_id[i], barcode[i], amount[i], order_region_id[i], unit[i], int(dates[i])] for i in range(num_all)]


#     # Storehouse

#     # 1 (store_id)

#     # 2 (amount)
#     store_amount_85 = int(n_size * 0.85)
#     store_amount_15 = n_size - store_amount_85
#     normal_part_store_amount  = rng.normal(20_000, 2_000, size=(store_amount_85,)).astype(np.int32)
#     uniform_part_store_amount = rng.uniform(10_000, 100_000, size=(store_amount_15)).astype(np.int32)
#     amount_in_store = np.concatenate((normal_part_store_amount, uniform_part_store_amount)) # use
#     rng.shuffle(amount_in_store)

#     store_amount = [amount_in_store[i] for i in range(n_size)]

#     meds_amount = str([f'med_good_{i} UInt32' for i in range(n_size)])[1:-1]
#     meds_amount = meds_amount.replace("'", "")

#     meds_insert = str([f'med_good_{i}' for i in range(n_size)])[1:-1]
#     meds_insert = meds_insert.replace("'", "")


#     client.execute(f'CREATE TABLE storehouse (store_id UInt32, {meds_amount}) ENGINE = MergeTree() PRIMARY KEY (store_id);')

#     client.execute(f"INSERT INTO logistics.storehouse (store_id, {meds_insert}) VALUES", [[0] + store_amount])
#     client.execute("INSERT INTO logistics.regions (region_id, region_name) VALUES", (i for i in data_regions))
#     client.execute("INSERT INTO logistics.medicines (med_id, artikul, nomenclature, weight, volume, price) VALUES", (i for i in data_medicines))

#     delay_orders = []

#     canceled_orders = []
#     name_canceled_orders = {}

#     for i in range(num_all):

#         # Если товар помечен как закончившийся, кладем заказ в отмененные
#         if data_orders[i][1] in name_canceled_orders:
#             canceled_orders.append(data_orders[i])
#             continue

#         # Смотрим сколько штук данного товара было приобретено
#         num_left = client.execute(f"SELECT sum(orders.amount) FROM logistics.orders WHERE orders.med_id == {data_orders[i][1]}")[0][0]

#         # Смотрим сколько данного товара осталось на складе 
#         num_left_storehouse = client.execute(f"SELECT med_good_{data_orders[i][1]} FROM logistics.storehouse")[0][0]

#         # Если товара больше нет на складе, кладем заказ в отмененные и помечаем что данного товара больше нет
#         if num_left_storehouse - num_left < 0:
#             canceled_orders.append(data_orders[i])
#             name_canceled_orders[data_orders[i][1]] = 'out_of_stock'
#             continue

#         # Если заказ не самый первый
#         if i != 0:
#             # Время предыдущего заказа
#             last_time_stamp = client.execute(f'SELECT date FROM logistics.orders WHERE order_id = {i - 1};')

#             last_time_stamp = int(last_time_stamp[0][0].timestamp())
            
#             # проверяем совпадает время последнего заказа с текущим
#             is_same_time = last_time_stamp == data_orders[i][6]

#             # если время с предыдущим совпадает добавляем его в отложенные заказы
#             if is_same_time:
#                 delay_orders.append(data_orders[i])
#                 continue

#             # если есть отложенные заказы, и время первого в отложенной очереди не совпадает с временем последнего товара вставляем его в заказы
#             if len(delay_orders) and last_time_stamp != delay_orders[0][6]:
#                 client.execute(f"INSERT INTO logistics.orders (order_id, med_id, barcode, amount, region_id, unit, date) VALUES", [delay_orders.pop(0)])

#         # Оформляем заказ
#         client.execute("INSERT INTO logistics.orders (order_id, med_id, barcode, amount, region_id, unit, date) VALUES", [data_orders[i]])

#         # Обновляем кол-во оставшихся товаров
#         # store_amount[data_orders[i][1]] -= data_orders[i][3]
#         # client.execute(f"INSERT INTO logistics.storehouse (store_id, {meds_insert}) VALUES", [[i + 1] + store_amount])


# client.execute('use logistics;')

# databases = client.execute("SHOW DATABASES;")
# tables    = client.execute("SHOW TABLES;")

# from_medicines = client.execute("SELECT * FROM medicines limit 5;")
# from_regions   = client.execute("SELECT * FROM regions limit 5;")
# from_orders    = client.execute("SELECT * FROM orders limit 5;")
# from_storehouse = client.execute("SELECT * FROM storehouse;")

# def dot_line():
#     print('------------------------------')

# print(f"Databases: {databases}")
# dot_line()
# print(f"Tables in logistics database: {tables}")
# dot_line()
# print("First 5 columns from medicines:")
# for i in from_medicines:
#     print(i)

# dot_line()
# print("First 5 columns from regions:")
# for i in from_regions:
#     print(i)

# dot_line()
# print("First 5 columns from orders:")
# for i in from_orders:
#     print(i)

# dot_line()
# print("First column from storehouse:")
# print(from_storehouse[0])

# dot_line()

# coop = client.execute("""SELECT orders.order_id AS id, med.nomenclature AS name, reg.region_name AS region, orders.date, med.price*orders.amount AS total_sum 
#                          FROM orders
#                          RIGHT JOIN regions AS reg ON reg.region_id = orders.region_id 
#                          RIGHT JOIN medicines AS med ON med.med_id = orders.med_id
#                          limit 5""")

# print("First 5 columns from coop (order_id, nomenclature, region, date, price*amount):")

# for i in coop:
#     print(i)

# print("Таблица с закончившимися лекарствами:\n", name_canceled_orders)
# print("Отмененные заказы:\n", canceled_orders)

# num_left_end = [client.execute(f"SELECT sum(orders.amount) FROM logistics.orders WHERE orders.med_id == {i}")[0][0] for i in range(n_size)]
# print(num_left_end)
