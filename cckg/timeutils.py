#! /usr/bin/env python

from calendar import monthrange
from datetime import date, datetime

from rdflib.namespace import XSD


def days_to_date(days, dtype):
    if dtype in {XSD.gMonth, XSD.gMonthDay}:
        # assume this year if non given
        year = date.today().year

        month, day = days_to_months(days, year)
        return "{}M-{}D".format(month, day)
    elif dtype in {XSD.gYear, XSD.gYearMonth}:
        year, day = days_to_years(days)

        if day > 31:
            month, day = days_to_months(day, year)
            return "{}Y-{}M-{}D".format(year,
                                        month,
                                        day)
        else:
            return "{}Y-{}D".format(year,
                                    day)
    else:
        # XSD.gDay
        return "{}D".format(days)

def days_to_years(days):
    # simplify to 365 days per year
    years = int(days/365)
    days_left = days%365

    return (years, days_left)

def days_to_months(days, year):
    days_per_month = [monthrange(year, m)[1] for m in range(1,13)]

    i = 1  # JAN
    while days > days_per_month[i-1]:
        days -= days_per_month[i-1]
        i += 1

        return (i, days)

    return (1, 1)

def gFrag_to_days(gFrag, dtype):
    if dtype is XSD.gMonth:
        # assume this year if non given
        year = datetime.today().year
        days_per_month = [monthrange(year, m)[1] for m in range(1, 13)]

        return sum(days_per_month[:int(gFrag)])
    elif dtype is XSD.gMonthDay:
        # assume this year if non given
        year = datetime.today().year
        days_per_month = [monthrange(year, m)[1] for m in range(1, 13)]

        m,d = gFrag.split('-')
        return sum(days_per_month[:int(m)])+int(d)
    elif dtype is XSD.gYear:
        # simplify to 365 days per year
        return int(gFrag)*365
    elif dtype is XSD.gYearMonth:
        # simplify days per month to this year
        year = datetime.today().year
        days_per_month = [monthrange(year, m)[1] for m in range(1, 13)]

        y,m = gFrag.split('-')
        return int(y)+sum(days_per_month[:int(m)])
    else:
        # XSD.gDay
        return int(gFrag)
