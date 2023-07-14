from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import aiopg
from sqlalchemy import MetaData
from typing import List, Optional
from datetime import datetime

app = FastAPI()


async def create_pool():
    return await aiopg.create_pool(
        "postgres://pcpjrkwk:1la04HSiD4BX-J8ewUdflOTt6aDx8z6D@trumpet.db.elephantsql.com/pcpjrkwk")


metadata = MetaData()


class User(BaseModel):
    id: int
    username: str
    phone: str
    email: str
    password: str
    address: str
    zip: str


class Consumer(BaseModel):
    id: int
    user_id: int
    type: str
    license: str


class Supplier(BaseModel):
    id: int
    user_id: int
    type: str
    masked: str
    license: str


class RequestedFood(BaseModel):
    id: int
    consumer_id: int
    category_type: int
    vegetarian_status: bool
    quantity: int
    active: bool
    time: Optional[datetime] = None


class SurplusFood(BaseModel):
    id: int
    supplier_id: int
    category_type: int
    vegetarian_status: bool
    quantity: int
    active: bool
    expiry: Optional[datetime] = None
    description: str


class Matching(BaseModel):
    id: int
    requested_food_id: int
    supplier_food_id: int
    status: str


class CategoryType(BaseModel):
    id: int
    category: str


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
                CREATE TABLE IF NOT EXISTS category_type (
                    id SERIAL PRIMARY KEY,
                    category VARCHAR(255) DEFAULT NULL
                )
            """)

            await cur.execute("""
                CREATE TABLE IF NOT EXISTS "user" (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(255) DEFAULT NULL,
                    phone VARCHAR(255) DEFAULT NULL,
                    email VARCHAR(255) DEFAULT NULL,
                    password VARCHAR(255) DEFAULT NULL,
                    address VARCHAR(255) DEFAULT NULL,
                    zip VARCHAR(255) DEFAULT NULL
                )
            """)
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS consumer (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES "user" (id) ON DELETE CASCADE,
                    type VARCHAR(255) DEFAULT NULL,
                    license VARCHAR(255) DEFAULT NULL
                )
            """)
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS supplier (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES "user" (id) ON DELETE CASCADE,
                    type VARCHAR(255) DEFAULT NULL,
                    masked VARCHAR(255) DEFAULT NULL,
                    license VARCHAR(255) DEFAULT NULL
                )
            """)
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS requested_food (
                    id SERIAL PRIMARY KEY,
                    consumer_id INTEGER REFERENCES consumer (id) ON DELETE CASCADE,
                    category_type INTEGER REFERENCES category_type (id) ON DELETE CASCADE,
                    vegetarian_status BOOLEAN DEFAULT NULL,
                    quantity INTEGER DEFAULT NULL,
                    active BOOLEAN DEFAULT NULL,
                    time TIMESTAMP DEFAULT NULL
                )
            """)
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS surplus_food (
                    id SERIAL PRIMARY KEY,
                    supplier_id INTEGER REFERENCES supplier (id) ON DELETE CASCADE,
                    category_type INTEGER REFERENCES category_type (id) ON DELETE CASCADE,
                    vegetarian_status BOOLEAN DEFAULT NULL,
                    quantity INTEGER DEFAULT NULL,
                    active BOOLEAN DEFAULT NULL,
                    expiry TIMESTAMP DEFAULT NULL,
                    description VARCHAR(255) DEFAULT NULL
                )
            """)

            await cur.execute("""
                CREATE TABLE IF NOT EXISTS matching (
                    id SERIAL PRIMARY KEY,
                    requested_food_id INTEGER REFERENCES requested_food (id) ON DELETE CASCADE,
                    supplier_food_id INTEGER REFERENCES surplus_food (id) ON DELETE CASCADE,
                    status VARCHAR(255) DEFAULT NULL
                )
            """)
            conn.commit()


@app.post("/users/", response_model=User)
async def create_user(user: User):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO \"user\" (username, phone, email, password, address, zip) "
                "VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
                (user.username, user.phone, user.email, user.password, user.address, user.zip)
            )
            user.id = (await cur.fetchone())[0]
            conn.commit()
    return user


@app.get("/users/{user_id}", response_model=User)
async def get_user(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM \"user\" WHERE id = %s",
                (user_id,)
            )
            result = await cur.fetchone()
            if result:
                user_data = {
                    "id": result[0],
                    "username": result[1],
                    "phone": result[2],
                    "email": result[3],
                    "password": result[4],
                    "address": result[5],
                    "zip": result[6]
                }
                return User(**user_data)
            else:
                raise HTTPException(status_code=404, detail="User not found")


@app.put("/users/{user_id}", response_model=User)
async def update_user(user_id: int, user: User):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE \"user\" SET username = %s, phone = %s, email = %s, password = %s, "
                "address = %s, zip = %s WHERE id = %s",
                (user.username, user.phone, user.email, user.password, user.address, user.zip, user_id)
            )
            conn.commit()
    return user


@app.delete("/users/{user_id}")
async def delete_user(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM \"user\" WHERE id = %s",
                (user_id,)
            )
            conn.commit()
    return {"message": "User deleted"}


@app.post("/consumers/", response_model=Consumer)
async def create_consumer(consumer: Consumer):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO consumer (user_id, type, license) "
                "VALUES (%s, %s, %s) RETURNING id",
                (consumer.user_id, consumer.type, consumer.license)
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
                    "user_id": result[1],
                    "type": result[2],
                    "license": result[3]
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
                "UPDATE consumer SET user_id = %s, type = %s, license = %s WHERE id = %s",
                (consumer.user_id, consumer.type, consumer.license, consumer_id)
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


@app.post("/suppliers/", response_model=Supplier)
async def create_supplier(supplier: Supplier):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO supplier (user_id, type, masked, license) "
                "VALUES (%s, %s, %s, %s) RETURNING id",
                (supplier.user_id, supplier.type, supplier.masked, supplier.license)
            )
            supplier.id = (await cur.fetchone())[0]
            conn.commit()
    return supplier


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
                    "user_id": result[1],
                    "type": result[2],
                    "masked": result[3],
                    "license": result[4]
                }
                return Supplier(**supplier_data)
            else:
                raise HTTPException(status_code=404, detail="Supplier not found")


@app.put("/suppliers/{supplier_id}", response_model=Supplier)
async def update_supplier(supplier_id: int, supplier: Supplier):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE supplier SET user_id = %s, type = %s, masked = %s, license = %s WHERE id = %s",
                (supplier.user_id, supplier.type, supplier.masked, supplier.license, supplier_id)
            )
            conn.commit()
    return supplier


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


@app.post("/requested-food/", response_model=RequestedFood)
async def create_requested_food(requested_food: RequestedFood):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # Check if time is provided, otherwise set it to the current timestamp
            if requested_food.time is None:
                requested_food.time = datetime.now()

            await cur.execute(
                "INSERT INTO requested_food (consumer_id, category_type, vegetarian_status, quantity, active, time) "
                "VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
                (requested_food.consumer_id, requested_food.category_type, requested_food.vegetarian_status,
                 requested_food.quantity, requested_food.active, requested_food.time)
            )
            requested_food.id = (await cur.fetchone())[0]
            conn.commit()
    return requested_food


@app.get("/requested-food/{requested_food_id}", response_model=RequestedFood)
async def get_requested_food(requested_food_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM requested_food WHERE id = %s",
                (requested_food_id,)
            )
            result = await cur.fetchone()
            if result:
                requested_food_data = {
                    "id": result[0],
                    "consumer_id": result[1],
                    "category_type": result[2],
                    "vegetarian_status": result[3],
                    "quantity": result[4],
                    "active": result[5],
                    "time": result[6]
                }
                return RequestedFood(**requested_food_data)
            else:
                raise HTTPException(status_code=404, detail="Requested food not found")


@app.put("/requested-food/{requested_food_id}", response_model=RequestedFood)
async def update_requested_food(requested_food_id: int, requested_food: RequestedFood):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE requested_food SET consumer_id = %s, category_type = %s, "
                "vegetarian_status = %s, quantity = %s, active = %s, time = %s "
                "WHERE id = %s",
                (requested_food.consumer_id, requested_food.category_type, requested_food.vegetarian_status,
                 requested_food.quantity, requested_food.active, requested_food.time, requested_food_id)
            )
            conn.commit()
    return requested_food


@app.delete("/requested-food/{requested_food_id}")
async def delete_requested_food(requested_food_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM requested_food WHERE id = %s",
                (requested_food_id,)
            )
            conn.commit()
    return {"message": "Requested food deleted"}


@app.post("/surplus-food/", response_model=SurplusFood)
async def create_surplus_food(surplus_food: SurplusFood):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # Check if expiry is provided, otherwise set it to the current timestamp
            if surplus_food.expiry is None:
                surplus_food.expiry = datetime.now()

            await cur.execute(
                "INSERT INTO surplus_food (supplier_id, category_type, vegetarian_status, quantity, active, expiry, description) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
                (surplus_food.supplier_id, surplus_food.category_type, surplus_food.vegetarian_status,
                 surplus_food.quantity, surplus_food.active, surplus_food.expiry, surplus_food.description)
            )
            surplus_food.id = (await cur.fetchone())[0]
            conn.commit()
    return surplus_food


@app.get("/surplus-food/{surplus_food_id}", response_model=SurplusFood)
async def get_surplus_food(surplus_food_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM surplus_food WHERE id = %s",
                (surplus_food_id,)
            )
            result = await cur.fetchone()
            if result:
                surplus_food_data = {
                    "id": result[0],
                    "supplier_id": result[1],
                    "category_type": result[2],
                    "vegetarian_status": result[3],
                    "quantity": result[4],
                    "active": result[5],
                    "expiry": result[6],
                    "description": result[7]
                }
                return SurplusFood(**surplus_food_data)
            else:
                raise HTTPException(status_code=404, detail="Surplus food not found")


@app.put("/surplus-food/{surplus_food_id}", response_model=SurplusFood)
async def update_surplus_food(surplus_food_id: int, surplus_food: SurplusFood):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE surplus_food SET supplier_id = %s, category_type = %s, "
                "vegetarian_status = %s, quantity = %s, active = %s, "
                " expiry = %s, description = %s "
                "WHERE id = %s",
                (surplus_food.supplier_id, surplus_food.category_type, surplus_food.vegetarian_status,
                 surplus_food.quantity, surplus_food.active,
                 surplus_food.expiry, surplus_food.description, surplus_food_id)
            )
            conn.commit()
    return surplus_food


@app.delete("/surplus-food/{surplus_food_id}")
async def delete_surplus_food(surplus_food_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM surplus_food WHERE id = %s",
                (surplus_food_id,)
            )
            conn.commit()
    return {"message": "Surplus food deleted"}


@app.post("/matching/", response_model=Matching)
async def create_matching(matching: Matching):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO matching (requested_food_id, supplier_food_id, status) "
                "VALUES (%s, %s, %s) RETURNING id",
                (matching.requested_food_id, matching.supplier_food_id, matching.status),
            )
            matching.id = (await cur.fetchone())[0]
            conn.commit()
    return matching


@app.get("/matching/{matching_id}", response_model=Matching)
async def get_matching(matching_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM matching WHERE id = %s",
                (matching_id,),
            )
            result = await cur.fetchone()
            if result:
                matching_data = {
                    "id": result[0],
                    "requested_food_id": result[1],
                    "supplier_food_id": result[2],
                    "status": result[3],
                }
                return Matching(**matching_data)
            else:
                raise HTTPException(status_code=404, detail="Matching not found")


@app.put("/matching/{matching_id}", response_model=Matching)
async def update_matching(matching_id: int, matching: Matching):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE matching SET requested_food_id = %s, supplier_food_id = %s, status = %s WHERE id = %s",
                (matching.requested_food_id, matching.supplier_food_id, matching.status, matching_id),
            )
            conn.commit()
    return matching


@app.delete("/matching/{matching_id}")
async def delete_matching(matching_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM matching WHERE id = %s",
                (matching_id,),
            )
            conn.commit()
    return {"message": "Matching deleted"}


@app.get("/users/", response_model=List[User])
async def get_all_users():
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM \"user\"")
            results = await cur.fetchall()
            users = []
            for result in results:
                user_data = {
                    "id": result[0],
                    "username": result[1],
                    "phone": result[2],
                    "email": result[3],
                    "password": result[4],
                    "address": result[5],
                    "zip": result[6],
                }
                users.append(User(**user_data))
            return users


@app.get("/consumers/", response_model=List[Consumer])
async def get_all_consumers():
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM consumer")
            results = await cur.fetchall()
            consumers = []
            for result in results:
                consumer_data = {
                    "id": result[0],
                    "user_id": result[1],
                    "type": result[2],
                    "license": result[3],
                }
                consumers.append(Consumer(**consumer_data))
            return consumers


@app.get("/suppliers/", response_model=List[Supplier])
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
                    "user_id": result[1],
                    "type": result[2],
                    "masked": result[3],
                    "license": result[4],
                }
                suppliers.append(Supplier(**supplier_data))
            return suppliers


@app.get("/requested-food/", response_model=List[RequestedFood])
async def get_all_requested_food():
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM requested_food")
            results = await cur.fetchall()
            requested_food_list = []
            for result in results:
                requested_food_data = {
                    "id": result[0],
                    "consumer_id": result[1],
                    "category_type": result[2],
                    "vegetarian_status": result[3],
                    "quantity": result[4],
                    "active": result[5],
                    "time": result[6],
                }
                requested_food_list.append(RequestedFood(**requested_food_data))
            return requested_food_list


@app.get("/surplus-food/", response_model=List[SurplusFood])
async def get_all_surplus_food():
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM surplus_food")
            results = await cur.fetchall()
            surplus_food_list = []
            for result in results:
                surplus_food_data = {
                    "id": result[0],
                    "supplier_id": result[1],
                    "category_type": result[2],
                    "vegetarian_status": result[3],
                    "quantity": result[4],
                    "active": result[5],
                    "expiry": result[6],
                    "description": result[7],
                }
                surplus_food_list.append(SurplusFood(**surplus_food_data))
            return surplus_food_list


@app.get("/matching/", response_model=List[Matching])
async def get_all_matching():
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM matching")
            results = await cur.fetchall()
            matching_list = []
            for result in results:
                matching_data = {
                    "id": result[0],
                    "requested_food_id": result[1],
                    "supplier_food_id": result[2],
                    "status": result[3],
                }
                matching_list.append(Matching(**matching_data))
            return matching_list


@app.post("/category-type/", response_model=CategoryType)
async def create_category_type(category_type: CategoryType):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO category_type (category) VALUES (%s) RETURNING id",
                (category_type.category,)
            )
            category_type.id = (await cur.fetchone())[0]
            conn.commit()
    return category_type


@app.get("/category-type/{category_type_id}", response_model=CategoryType)
async def get_category_type(category_type_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM category_type WHERE id = %s",
                (category_type_id,)
            )
            result = await cur.fetchone()
            if result:
                category_type_data = {
                    "id": result[0],
                    "category": result[1]
                }
                return CategoryType(**category_type_data)
            else:
                raise HTTPException(status_code=404, detail="Category type not found")


@app.put("/category-type/{category_type_id}", response_model=CategoryType)
async def update_category_type(category_type_id: int, category_type: CategoryType):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE category_type SET category = %s WHERE id = %s",
                (category_type.category, category_type_id)
            )
            conn.commit()
    return category_type


@app.delete("/category-type/{category_type_id}")
async def delete_category_type(category_type_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM category_type WHERE id = %s",
                (category_type_id,)
            )
            conn.commit()
    return {"message": "Category type deleted"}


@app.get("/category-type/", response_model=List[CategoryType])
async def get_all_category_types():
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM category_type"
            )
            category_types = []
            async for result in cur:
                category_type_data = {
                    "id": result[0],
                    "category": result[1]
                }
                category_types.append(CategoryType(**category_type_data))
            return category_types


from fastapi import Path


@app.delete("/clear_table/{table_name}")
async def clear_table(table_name: str = Path(..., description="Name of the table to clear")):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"DELETE FROM {table_name}")
            conn.commit()

    return {"message": f"All data from {table_name} table has been cleared."}


from fastapi import Path


@app.delete("/clear_table/{table_name}")
async def clear_table(table_name: str = Path(..., description="Name of the table to clear")):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"DELETE FROM {table_name}")
            conn.commit()

    return {"message": f"All data from {table_name} table has been cleared."}


@app.delete("/delete_tables")
async def delete_tables():
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # Drop matching table
            await cur.execute("DROP TABLE IF EXISTS matching")
            # Drop surplus_food table
            await cur.execute("DROP TABLE IF EXISTS surplus_food")
            # Drop requested_food table
            await cur.execute("DROP TABLE IF EXISTS requested_food")
            # Drop supplier table
            await cur.execute("DROP TABLE IF EXISTS supplier")
            # Drop consumer table
            await cur.execute("DROP TABLE IF EXISTS consumer")
            # Drop user table
            await cur.execute("DROP TABLE IF EXISTS \"user\"")

            conn.commit()

    return {"message": "All tables have been deleted."}