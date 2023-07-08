from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import aiopg
from sqlalchemy import Column, Integer, String, MetaData, Table, ForeignKey
from typing import List

app = FastAPI()

async def create_pool():
    return await aiopg.create_pool("postgres://sourav:mZ5mLQG4wwWcmz3oF02LA3reD7ukm7qL@dpg-cig640d9aq012etvhjt0-a.oregon-postgres.render.com/consumer")

metadata = MetaData()

consumer_table = Table(
    "consumer",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("conn_type", String(255)),
    Column("phone_no", String(255)),
    Column("email", String(255)),
    Column("password", String(255))
)

conn_individual_table = Table(
    "conn_individual",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("ini_name", String(255)),
    Column("add", String(255)),
    Column("zip", String(255)),
    Column("consumer_id", Integer, ForeignKey("consumer.id"))
)

conn_ngo_table = Table(
    "conn_ngo",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("ngo_name", String(255)),
    Column("add", String(255)),
    Column("zip", String(255)),
    Column("licence", String(255)),
    Column("consumer_id", Integer, ForeignKey("consumer.id"))
)

supplier_table = Table(
    "supplier",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(255)),
    Column("type", String(255)),
    Column("phone", String(255)),
    Column("email", String(255)),
    Column("password", String(255))
)

fnb_table = Table(
    "fnb",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(255)),
    Column("address", String(255)),
    Column("zip", String(255)),
    Column("licence", String(255)),
    Column("supplier_id", Integer, ForeignKey("supplier.id"))
)

individual_table = Table(
    "individual",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("address", String(255)),
    Column("zip", String(255)),
    Column("supplier_id", Integer, ForeignKey("supplier.id"))
)

class Consumer(BaseModel):
    id: int
    conn_type: str
    phone_no: str
    email: str
    password: str

class ConnIndividual(BaseModel):
    id: int
    ini_name: str
    add: str
    zip: str
    consumer_id: int

class ConnNgo(BaseModel):
    id: int
    ngo_name: str
    add: str
    zip: str
    licence: str
    consumer_id: int

class Supplier(BaseModel):
    id: int
    name: str
    type: str
    phone: str
    email: str
    password: str

class Fnb(BaseModel):
    id: int
    name: str
    address: str
    zip: str
    licence: str
    supplier_id: int

class Individual(BaseModel):
    id: int
    address: str
    zip: str
    supplier_id: int


async def get_pool():
    if not hasattr(app.state, 'db_pool'):
        app.state.db_pool = await create_pool()
    return app.state.db_pool

@app.on_event("startup")
async def startup_event():
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS consumer (
                    id SERIAL PRIMARY KEY,
                    conn_type VARCHAR(255),
                    phone_no VARCHAR(255),
                    email VARCHAR(255),
                    password VARCHAR(255)
                )
            """)
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS conn_individual (
                    id SERIAL PRIMARY KEY,
                    ini_name VARCHAR(255),
                    add VARCHAR(255),
                    zip VARCHAR(255),
                    consumer_id INTEGER REFERENCES consumer (id)
                )
            """)
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS conn_ngo (
                    id SERIAL PRIMARY KEY,
                    ngo_name VARCHAR(255),
                    add VARCHAR(255),
                    zip VARCHAR(255),
                    licence VARCHAR(255),
                    consumer_id INTEGER REFERENCES consumer (id)
                )
            """)
            conn.commit()

@app.post("/consumers/", response_model=Consumer)
async def create_consumer(consumer: Consumer):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO consumer (conn_type, phone_no, email, password) "
                "VALUES (%s, %s, %s, %s) RETURNING id",
                (consumer.conn_type, consumer.phone_no, consumer.email, consumer.password)
            )
            consumer.id = (await cur.fetchone())[0]
            conn.commit()
    return consumer

@app.get("/consumers/{consumer_id}", response_model=Consumer)
async def get_consumer(consumer_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM consumer WHERE id = %s",
                (consumer_id,)
            )
            result = await cur.fetchone()
            if result:
                consumer_data = {
                    "id": result[0],
                    "conn_type": result[1],
                    "phone_no": result[2],
                    "email": result[3],
                    "password": result[4]
                }
                return Consumer(**consumer_data)
            else:
                raise HTTPException(status_code=404, detail="Consumer not found")


@app.put("/consumers/{consumer_id}", response_model=Consumer)
async def update_consumer(consumer_id: int, consumer: Consumer):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE consumer SET conn_type = %s, phone_no = %s, email = %s, password = %s "
                "WHERE id = %s",
                (consumer.conn_type, consumer.phone_no, consumer.email, consumer.password, consumer_id)
            )
            conn.commit()
    return consumer

@app.delete("/consumers/{consumer_id}")
async def delete_consumer(consumer_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM consumer WHERE id = %s",
                (consumer_id,)
            )
            conn.commit()
    return {"message": "Consumer deleted"}

@app.get("/conn_individual/", response_model=List[ConnIndividual])
async def get_all_conn_individuals():
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM conn_individual")
            results = await cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            conn_individuals = []
            for result in results:
                conn_individual_data = dict(zip(column_names, result))
                conn_individuals.append(ConnIndividual(**conn_individual_data))
            return conn_individuals


@app.post("/conn_individual/", response_model=ConnIndividual)
async def create_conn_individual(conn_individual: ConnIndividual):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO conn_individual (ini_name, add, zip, consumer_id) "
                "VALUES (%s, %s, %s, %s) RETURNING id",
                (conn_individual.ini_name, conn_individual.add, conn_individual.zip, conn_individual.consumer_id)
            )
            conn_individual.id = (await cur.fetchone())[0]
            conn.commit()
    return conn_individual

@app.get("/conn_individual/{conn_individual_id}", response_model=ConnIndividual)
async def get_conn_individual(conn_individual_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM conn_individual WHERE id = %s",
                (conn_individual_id,)
            )
            result = await cur.fetchone()
            if result:
                conn_individual_data = {
                    "id": result[0],
                    "ini_name": result[1],
                    "add": result[2],
                    "zip": result[3],
                    "consumer_id": result[4]
                }
                return ConnIndividual(**conn_individual_data)
            else:
                raise HTTPException(status_code=404, detail="Connection Individual not found")

@app.put("/conn_individual/{conn_individual_id}", response_model=ConnIndividual)
async def update_conn_individual(conn_individual_id: int, conn_individual: ConnIndividual):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE conn_individual SET ini_name = %s, add = %s, zip = %s, consumer_id = %s "
                "WHERE id = %s",
                (conn_individual.ini_name, conn_individual.add, conn_individual.zip, conn_individual.consumer_id, conn_individual_id)
            )
            conn.commit()
    return conn_individual

@app.delete("/conn_individual/{conn_individual_id}")
async def delete_conn_individual(conn_individual_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM conn_individual WHERE id = %s",
                (conn_individual_id,)
            )
            conn.commit()
    return {"message": "Connection Individual deleted"}

@app.get("/conn_individual/", response_model=List[ConnIndividual])
async def get_all_conn_individuals():
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM conn_individual")
            results = await cur.fetchall()
            conn_individuals = [ConnIndividual(**dict(zip(cur.description, result))) for result in results]
            return conn_individuals

@app.post("/conn_ngo/", response_model=ConnNgo)
async def create_conn_ngo(conn_ngo: ConnNgo):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO conn_ngo (ngo_name, add, zip, licence, consumer_id) "
                "VALUES (%s, %s, %s, %s, %s) RETURNING id",
                (conn_ngo.ngo_name, conn_ngo.add, conn_ngo.zip, conn_ngo.licence, conn_ngo.consumer_id)
            )
            conn_ngo.id = (await cur.fetchone())[0]
            conn.commit()
    return conn_ngo

@app.get("/conn_ngo/{conn_ngo_id}", response_model=ConnNgo)
async def get_conn_ngo(conn_ngo_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM conn_ngo WHERE id = %s",
                (conn_ngo_id,)
            )
            result = await cur.fetchone()
            if result:
                conn_ngo_data = {
                    "id": result[0],
                    "ngo_name": result[1],
                    "add": result[2],
                    "zip": result[3],
                    "licence": result[4],
                    "consumer_id": result[5]
                }
                return ConnNgo(**conn_ngo_data)
            else:
                raise HTTPException(status_code=404, detail="Connection NGO not found")


@app.put("/conn_ngo/{conn_ngo_id}", response_model=ConnNgo)
async def update_conn_ngo(conn_ngo_id: int, conn_ngo: ConnNgo):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE conn_ngo SET ngo_name = %s, add = %s, zip = %s, licence = %s, consumer_id = %s "
                "WHERE id = %s",
                (conn_ngo.ngo_name, conn_ngo.add, conn_ngo.zip, conn_ngo.licence, conn_ngo.consumer_id, conn_ngo_id)
            )
            conn.commit()
    return conn_ngo

@app.delete("/conn_ngo/{conn_ngo_id}")
async def delete_conn_ngo(conn_ngo_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM conn_ngo WHERE id = %s",
                (conn_ngo_id,)
            )
            conn.commit()
    return {"message": "Connection NGO deleted"}

@app.get("/conn_ngo/", response_model=List[ConnNgo])
async def get_all_conn_ngos():
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM conn_ngo")
            results = await cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            conn_ngos = []
            for result in results:
                conn_ngo_data = dict(zip(column_names, result))
                conn_ngos.append(ConnNgo(**conn_ngo_data))
            return conn_ngos


# POST endpoint to create a new supplier
@app.post("/suppliers/", response_model=Supplier)
async def create_supplier(supplier: Supplier):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO supplier (name, type, phone, email, password) "
                "VALUES (%s, %s, %s, %s, %s) RETURNING id",
                (supplier.name, supplier.type, supplier.phone, supplier.email, supplier.password)
            )
            supplier.id = (await cur.fetchone())[0]
            conn.commit()
    return supplier

# GET endpoint to retrieve a supplier
@app.get("/suppliers/{supplier_id}", response_model=Supplier)
async def get_supplier(supplier_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM supplier WHERE id = %s",
                (supplier_id,)
            )
            result = await cur.fetchone()
            if result:
                supplier_data = {
                    "id": result[0],
                    "name": result[1],
                    "type": result[2],
                    "phone": result[3],
                    "email": result[4],
                    "password": result[5]
                }
                return Supplier(**supplier_data)
            else:
                raise HTTPException(status_code=404, detail="Supplier not found")


# PUT endpoint to update a supplier
@app.put("/suppliers/{supplier_id}", response_model=Supplier)
async def update_supplier(supplier_id: int, supplier: Supplier):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE supplier SET name = %s, type = %s, phone = %s, email = %s, password = %s "
                "WHERE id = %s",
                (supplier.name, supplier.type, supplier.phone, supplier.email, supplier.password, supplier_id)
            )
            conn.commit()
    return supplier

# DELETE endpoint to delete a supplier
@app.delete("/suppliers/{supplier_id}")
async def delete_supplier(supplier_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM supplier WHERE id = %s",
                (supplier_id,)
            )
            conn.commit()
    return {"message": "Supplier deleted"}


# POST endpoint to create a new fnb
@app.post("/fnb/", response_model=Fnb)
async def create_fnb(fn: Fnb):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO fnb (supplier_id, name, address, zip, licence) "
                "VALUES (%s, %s, %s, %s, %s) RETURNING id",
                (fn.supplier_id, fn.name, fn.address, fn.zip, fn.licence)
            )
            fn.id = (await cur.fetchone())[0]
            conn.commit()
    return fn

# GET endpoint to retrieve an fnb
@app.get("/fnb/{fnb_id}", response_model=Fnb)
async def get_fnb(fnb_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM fnb WHERE id = %s",
                (fnb_id,)
            )
            result = await cur.fetchone()
            if result:
                fnb_data = {
                    "id": result[0],
                    "supplier_id": result[1],
                    "name": result[2],
                    "address": result[3],
                    "zip": result[4],
                    "licence": result[5]
                }
                return Fnb(**fnb_data)
            else:
                raise HTTPException(status_code=404, detail="Fnb not found")


# PUT endpoint to update an fnb
@app.put("/fnb/{fnb_id}", response_model=Fnb)
async def update_fnb(fnb_id: int, fnb: Fnb):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE fnb SET supplier_id = %s, name = %s, address = %s, zip = %s, licence = %s "
                "WHERE id = %s",
                (fnb.supplier_id, fnb.name, fnb.address, fnb.zip, fnb.licence, fnb_id)
            )
            conn.commit()
    return fnb

# DELETE endpoint to delete an fnb
@app.delete("/fnb/{fnb_id}")
async def delete_fnb(fnb_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM fnb WHERE id = %s",
                (fnb_id,)
            )
            conn.commit()
    return {"message": "Fnb deleted"}


# POST endpoint to create a new individual
@app.post("/individuals/", response_model=Individual)
async def create_individual(individual: Individual):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO individual (supplier_id, address, zip) "
                "VALUES (%s, %s, %s) RETURNING id",
                (individual.supplier_id, individual.address, individual.zip)
            )
            individual.id = (await cur.fetchone())[0]
            conn.commit()
    return individual

# GET endpoint to retrieve an individual
@app.get("/individuals/{individual_id}", response_model=Individual)
async def get_individual(individual_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM individual WHERE id = %s",
                (individual_id,)
            )
            result = await cur.fetchone()
            if result:
                individual_data = {
                    "id": result[0],
                    "supplier_id": result[1],
                    "address": result[2],
                    "zip": result[3]
                }
                return Individual(**individual_data)
            else:
                raise HTTPException(status_code=404, detail="Individual not found")


# PUT endpoint to update an individual
@app.put("/individuals/{individual_id}", response_model=Individual)
async def update_individual(individual_id: int, individual: Individual):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE individual SET supplier_id = %s, address = %s, zip = %s "
                "WHERE id = %s",
                (individual.supplier_id, individual.address, individual.zip, individual_id)
            )
            conn.commit()
    return individual

# DELETE endpoint to delete an individual
@app.delete("/individuals/{individual_id}")
async def delete_individual(individual_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM individual WHERE id = %s",
                (individual_id,)
            )
            conn.commit()
    return {"message": "Individual deleted"}

# GET endpoint to retrieve all suppliers
@app.get("/suppliers", response_model=List[Supplier])
async def get_all_suppliers():
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM supplier")
            results = await cur.fetchall()
            suppliers = []
            for result in results:
                supplier_data = {
                    "id": result[0],
                    "name": result[1],
                    "type": result[2],
                    "phone": result[3],
                    "email": result[4],
                    "password": result[5]
                }
                suppliers.append(Supplier(**supplier_data))
            return suppliers

# GET endpoint to retrieve all fnbs
@app.get("/fnbs", response_model=List[Fnb])
async def get_all_fnbs():
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM fnb")
            results = await cur.fetchall()
            fnbs = []
            for result in results:
                fnb_data = {
                    "id": result[0],
                    "supplier_id": result[1],
                    "name": result[2],
                    "address": result[3],
                    "zip": result[4],
                    "licence": result[5]
                }
                fnbs.append(Fnb(**fnb_data))
            return fnbs

# GET endpoint to retrieve all individuals
@app.get("/individuals", response_model=List[Individual])
async def get_all_individuals():
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM individual")
            results = await cur.fetchall()
            individuals = []
            for result in results:
                individual_data = {
                    "id": result[0],
                    "supplier_id": result[1],
                    "address": result[2],
                    "zip": result[3]
                }
                individuals.append(Individual(**individual_data))
            return individuals
