from typing import Optional

import polars as pl
from faker import Faker

Faker.seed(4321)

faker = Faker()


def generate_fake_dataframe():
    record_count = faker.random_int(min=4000, max=9000)
    facto = faker.random_int(min=9, max=19)
    data = [
        pl.Series(name="lastModifiedDate", values=fake_dates(record_count * facto), dtype=pl.Date),
        pl.Series(name="Size", values=fake_numbers(2222 + (record_count * facto), 50000 + (record_count * facto),
                                                   record_count * facto), dtype=pl.Int64),
    ]
    inventory_df = pl.DataFrame(data)
    return inventory_df


def fake_numbers(low: Optional[int], high: Optional[int], count):
    return [faker.random_int(min=low, max=high) for i in range(count)]


def fake_dates(count):
    return [faker.date_object() for i in range(count)]
