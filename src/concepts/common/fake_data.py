from datetime import datetime

import polars as pl
from mimesis import *

# Set number of rows and chunks for the Polars to create
n_rows = 20_000_000
n_chunks = 15
chunk_size = int(n_rows / n_chunks)

# Set date range for the dataset
start_date = datetime(2022, 1, 1)
end_date = datetime(2022, 12, 31)

generic = Generic(locale=Locale.EN)

generic.person.username()
# Output: 'sherley3354'

generic.datetime.date()
# Output: '14-05-2007'
from mimesis import Fieldset
from mimesis.locales import Locale


def generate_fake_dataframe(count: int = 5000, weighted: bool = False):
    if weighted:
        weight = generic.random.randint(a=2, b=9)
    else:
        weight = 1
    row_count = count * weight
    fs = Fieldset(locale=Locale.EN, i=row_count)
    low = generic.random.randint(8000, 90_000)
    high = generic.random.randint(900_000, 12_000_000)
    weighted_low = low + (count * weight)
    weighted_high = high + (count * weight)
    return pl.from_dict({
        "ID": fs("increment"),
        "Name": fs("person.full_name"),
        "Email": fs("email"),
        "Phone": fs("telephone", mask="+1 (###) #5#-7#9#"),
        "Path": fs("internet.uri"),
        "Size": fs("random.randint", a=weighted_low, b=weighted_high),
        "lastModifiedDate": fs("datetime.date", start=2023, end=2023)
    })
