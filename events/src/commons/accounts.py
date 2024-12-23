import os
import numpy as np
from uuid import uuid4
from faker import Faker


fake = Faker("pt_BR")


class AccountsEvents:
    @staticmethod
    def generate_account(x: object) -> object:
        return {
            data: {
                "account_id": str(uuid4()),
                "account_type": np.random.choice(["personal", "business"]),
                "balance": np.random.randint(1, 1000),
                "currency": np.random.choice(["BRL", "USD"]),
                "status": np.random.choice(["active", "inactive"]),
                "user_id": str(uuid4()),
            }
            for data in range(x)
        }

    @staticmethod
    def generate_subaccount(x: object) -> object:
        return {
            data: {
                "subaccount_id": str(uuid4()),
                "parent_account_id": str(uuid4()),
                "purpose": np.random.choice(["savings", "investment"]),
                "balance": np.random.randint(1, 1000),
            }
            for data in range(x)
        }

    @staticmethod
    def generate_user(x: object) -> object:
        return {
            data: {
                "user_id": str(uuid4()),
                "name": fake.name(),
                "email": fake.email(),
                "phone": fake.phone_number(),
                "created_at": fake.date_time_this_year(),
            }
            for data in range(x)
        }
