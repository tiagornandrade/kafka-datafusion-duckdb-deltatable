import os
import numpy as np
from uuid import uuid4
from faker import Faker


fake = Faker("pt_BR")


class AccountEvents:
    @staticmethod
    def generate_transaction(x: object) -> object:
        return {
            data: {
                "transaction_id": str(uuid4()),
                "amount": np.random.randint(1, 1000),
                "currency": "BRL",
                "status": np.random.choice(["approved", "declined"]),
                "timestamp": fake.date_time_this_year(),
                "sender_id": str(uuid4()),
                "receiver_id": str(uuid4()),
            }
            for data in range(x)
        }

    @staticmethod
    def generate_payment_method(x: object) -> object:
        return {
            data: {
                "method_id": str(uuid4()),
                "type": np.random.choice(["credit card", "PIX"]),
                "details": fake.credit_card_number(),
                "user_id": str(uuid4()),
            }
            for data in range(x)
        }

    @staticmethod
    def generate_merchant(x: object) -> object:
        return {
            data: {
                "merchant_id": str(uuid4()),
                "name": fake.company(),
                "category": np.random.choice(["restaurant", "market", "pharmacy"]),
                "contact_info": fake.phone_number(),
            }
            for data in range(x)
        }
