# standard
import logging

def latinized(log_entries: list[(str,str,str)] = None, logger: logging.Logger = None) -> None:

    if not log_entries:
        return

    row_width = 50

    rows = list()
    for entry in log_entries:
        entry_string = f"| {repr(entry[1]) :<{row_width-2}} | {repr(entry[2]) :<{row_width-2}} |"
        if entry[0] and entry_string not in rows:
            rows += [entry_string]

    latinized_header = f"\nTHE FOLLOWING CYRILLIC NAMES WERE TRANSLITERATED TO LATIN:\n"\
    f"+{'':-<{row_width}}+{'':-<{row_width}}+\n"\
    f"| {'original cyrillic':<{row_width-2}} | {'latinized replacement':<{row_width-2}} |\n"\
    f"+{'':-<{row_width}}+{'':-<{row_width}}+"

    logger.info(latinized_header)
    for row in rows:
        logger.info(row)
    logger.info(f"+{'':-<{row_width}}+{'':-<{row_width}}+")


def parse_fail(n_total: int, log_entries: list[(str,str)] = None, logger: logging.Logger = None) -> None:

    if not log_entries:
        return

    row_width = 50
    n_failed_entries = len(log_entries)

    rows = list()
    for entry in log_entries:
        entry_string = f"| {repr(entry[0]) :<{row_width-2}} | {str(entry[1]) :<{row_width-2}} |"
        if entry[0] and entry_string not in rows:
            rows += [entry_string]

    parse_fail_header = f"\nCOULD NOT PARSE AUTHORS FROM FOLLOWING ENTRIES\n"\
    f"({round(n_failed_entries / n_total * 100)} % of {n_total}. {len(rows)} unique entries):\n"\
    f"+{'':-<{row_width}}+{'':-<{row_width}}+\n"\
    f"| {'raw authors string':<{row_width-2}} | {'cleaned authors string':<{row_width-2}} |\n"\
    f"+{'':-<{row_width}}+{'':-<{row_width}}+"

    logger.info(parse_fail_header)
    for row in rows:
        logger.info(row)
    logger.info(f"+{'':-<{row_width}}+{'':-<{row_width}}+")
