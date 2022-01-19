class DateFormatSelector(object):

    @staticmethod
    def timestamp(year: int, month: int, day: int,
                  hour: int = 0, minute: int = 0, second: float = 0.0):

        return (
            f"timestamp '{year}-{month:02d}-{day:02d} "
            f"{hour:02d}:{minute:02d}:{second:06.3f}'"
        )

    @staticmethod
    def year(column: str,
             as_name: str = 'month') -> str:
        """
        Year, numeric, four digits
        """
        return f"date_format({column}, '%Y') as {as_name}"

    @staticmethod
    def year_2(column: str,
               as_name: str = 'month') -> str:
        """
        Year, numeric (two digits)
        """
        return f"date_format({column}, '%y') as {as_name}"

    @staticmethod
    def month(column: str,
              as_name: str = 'month') -> str:
        """
        Month, numeric (1 .. 12)
        """
        return f"date_format({column}, '%c') as {as_name}"

    @staticmethod
    def month_name(column: str,
                   as_name: str = 'month_name') -> str:
        """
        Month name (January .. December)
        """
        return f"date_format({column}, '%M') as {as_name}"

    @staticmethod
    def month_name_3(column: str,
                     as_name: str = 'month_name') -> str:
        """
        Abbreviated month name (Jan .. Dec)
        """
        return f"date_format({column}, '%b') as {as_name}"

    @staticmethod
    def week(column: str,
             as_name: str = 'week') -> str:
        """
        Month, numeric (1 .. 12)
        """
        return f"date_format({column}, '%c') as {as_name}"

    @staticmethod
    def weekday_name(column: str,
                     as_name: str = 'weekday_name') -> str:
        """
        Weekday name (Sunday .. Saturday)
        """
        return f"date_format({column}, '%W') as '{as_name}'"

    @staticmethod
    def weekday_name_3(column: str,
                       as_name: str = 'weekday_name') -> str:
        """
        Abbreviated weekday name (Sun .. Sat)
        """
        return f"date_format({column}, '%a') as {as_name}"

    @staticmethod
    def day_of_year(column: str,
                    as_name: str = 'day') -> str:
        """
        Day of year (001 .. 366)
        """
        return f"date_format({column}, '%j') as {as_name}"

    @staticmethod
    def day_of_month(column: str,
                     as_name: str = 'day') -> str:
        """
        Day of the month, numeric (01 .. 31)
        """
        return f"date_format({column}, '%e') as {as_name}"

    @staticmethod
    def day_of_week(column: str,
                    as_name: str = 'day') -> str:
        """
        Day of the week (0 .. 6), where Sunday is the first day of the week
        """
        return f"date_format({column}, '%w') as {as_name}"

    @staticmethod
    def hour_24(column: str,
                as_name: str = 'hour') -> str:
        """
        Hour (00 .. 23)
        """
        return f"date_format({column}, '%H') as {as_name}"

    @staticmethod
    def hour_12(column: str,
                as_name: str = 'hour') -> str:
        """
        Hour (01 .. 12)
        """
        return f"date_format({column}, '%h') as {as_name}"

    @staticmethod
    def am_pm(column: str,
              as_name: str = 'hour') -> str:
        """
        AM or PM
        """
        return f"date_format({column}, '%p') as {as_name}"

    @staticmethod
    def minute(column: str,
               as_name: str = 'hour') -> str:
        """
        Minutes, numeric (00 .. 59)
        """
        return f"date_format({column}, '%h') as {as_name}"

    @staticmethod
    def second(column: str,
               as_name: str = 'second') -> str:
        """
        Seconds (00 .. 59)
        """
        return f"date_format({column}, '%s') as {as_name}"

    @staticmethod
    def time(column: str,
             as_name: str = 'time') -> str:
        """
        Time, 24-hour (hh:mm:ss)
        """
        return f"date_format({column}, '%T') as {as_name}"
