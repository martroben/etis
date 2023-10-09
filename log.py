# standard
import logging
import time

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
    f"{round(n_failed_entries / n_total * 100)} % of {n_total}, {len(rows)} unique entries:\n"\
    f"+{'':-<{row_width}}+{'':-<{row_width}}+\n"\
    f"| {'raw authors string':<{row_width-2}} | {'cleaned authors string':<{row_width-2}} |\n"\
    f"+{'':-<{row_width}}+{'':-<{row_width}}+"

    logger.info(parse_fail_header)
    for row in rows:
        logger.info(row)
    logger.info(f"+{'':-<{row_width}}+{'':-<{row_width}}+")


def merge_total_result(n_initial: int, n_merged: int, logger: logging.Logger) -> None:
    logger.info(f"\nMerging author aliases finished.\n"
                f"Merged a total of {n_merged} out of {n_initial} initial aliases.")


def merge_cycle_result(n_initial, n_merged, time_s, logger: logging.Logger) -> None:
    time_string = f"{round(time_s)} seconds" if time_s < 120 else f"{round(time_s / 60, 1)} minutes"
    logger.info(f"\nMerging author aliases cycle finished.\n"
                f"Merged {n_merged} out of {n_initial} aliases during the cycle.\n"
                f"Cycle time: {time_string}.")


def api_result(n_pulled, start_time, logger):
    logger.info(f"records pulled: {n_pulled}, time elapsed: {round((time.time() - start_time) / 60, 2)} minutes")
