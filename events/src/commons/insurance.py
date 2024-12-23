import os
import numpy as np
from uuid import uuid4
from faker import Faker


fake = Faker("pt_BR")


class AccountsEvents:
    @staticmethod
    def generate_policy(x: object) -> object:
        return {
            data: {
                "policy_id": str(uuid4()),
                "type": np.random.choice(["auto", "home"]),
                "coverage_amount": np.random.randint(1, 1000),
                "premium": np.random.randint(1, 1000),
                "start_date": fake.date_time_this_year(),
                "end_date": fake.date_time_this_year(),
                "user_id": str(uuid4()),
            }
            for data in range(x)
        }

    @staticmethod
    def generate_claim(x: object) -> object:
        return {
            data: {
                "claim_id": str(uuid4()),
                "policy_id": str(uuid4()),
                "amount_claimed": np.random.randint(1, 1000),
                "status": np.random.choice(["pending", "approved", "rejected"]),
                "filed_date": fake.date_this_year(),
            }
            for data in range(x)
        }

    @staticmethod
    def generate_insured_entity(x: object) -> object:
        return {
            data: {
                "entity_id": str(uuid4()),
                "type": np.random.choice(["vehicle", "property"]),
                "description": fake.word(),
                "value": np.random.randint(1, 1000),
            }
            for data in range(x)
        }
