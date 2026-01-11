from db.connection import get_connection


class Customer:
    def __init__(self, cust_id: int, name: str, email: str) -> None:
        self.id = cust_id
        self.name = name
        self.email = email

    def __str__(self) -> str:
        return f"Customer({self.id}, {self.name}, {self.email})"


def create_table() -> None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS customers")
        cur.execute(
            """
                CREATE TABLE customers (
                    id serial PRIMARY KEY,
                    name varchar not null,
                    email varchar not null unique)
                """,
        )
        conn.commit()


def create_customer(name: str, email: str) -> None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO customers (name, email) VALUES (%s, %s)",
            (name, email),
        )
        conn.commit()


def get_all_customers() -> list[Customer]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM customers")
        return [Customer(cid, name, email) for cid, name, email in cur]


def get_customer_by_email(email: str) -> Customer | None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, name, email FROM customers WHERE email = %s",
            (email,),
        )
        result = cur.fetchone()
        if result:
            cid, name, email = result
            return Customer(cid, name, email)
        return None


def delete_all_customers() -> None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM customers")
        conn.commit()
