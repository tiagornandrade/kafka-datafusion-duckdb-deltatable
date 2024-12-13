import os
import numpy as np
from uuid import uuid4
from faker import Faker


fake = Faker("pt_BR")


class LendingsEvents:
    @staticmethod
    def generate_loan(x: int) -> dict[str, dict]:
        return {
            data: {
                "loan_id": str(uuid4()),
                "amount": np.random.randint(1000, 10000),
                "interest_rate": np.random.uniform(0.05, 0.20),
                "duration": np.random.randint(12, 36),
                "start_date": fake.date_this_year(),
                "borrower_id": str(uuid4()),
                "status": np.random.choice(["pending", "approved", "rejected"]),
            }
            for data in range(x)
        }

    @staticmethod
    def generate_borrower(x: object) -> object:
        return {
            data: {
                "borrower_id": str(uuid4()),
                "name": fake.name(),
                "credit_score": np.random.randint(300, 850),
                "income": np.random.randint(1000, 10000),
                "contact_info": fake.phone_number(),
            }
            for data in range(x)
        }

    @staticmethod
    def generate_repayment(x: object) -> object:
        return {
            data: {
                "repayment_id": str(uuid4()),
                "loan_id": str(uuid4()),
                "amount_paid": np.random.randint(100, 1000),
                "due_date": fake.date_this_year(),
                "payment_date": fake.date_this_year(),
            }
            for data in range(x)
        }
