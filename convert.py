import collections
import re
import enum
import csv

SPLIT_RE = re.compile(r"Split \((\d+)/(\d+)\) ?")
export_path = "export.csv"
converted_path = "converted.csv"


class ParserState(enum.IntEnum):
    new_transaction = enum.auto()
    inside_transaction = enum.auto()


def rename_row(row, src, dst):
    row[dst] = row[src]
    del row[src]


def read_rows():
    with open(export_path) as fd:
        reader = csv.DictReader(
            fd,
            delimiter=';',
        )
        return list(reader)


def iter_rows(rows):
    """
    Iterate over rows.

    - Date
    - Memo
    - Transaction ID
    - Description
    - Account
    - Deposit
    - Withdrawal
    """
    state = ParserState.new_transaction
    num = 0
    for index, row in enumerate(rows):
        memo = row["Memo"]
        split_parameters = SPLIT_RE.match(memo)
        is_split = bool(split_parameters)

        if state == ParserState.new_transaction:
            if is_split:
                state = ParserState.inside_transaction
            num += 1
        elif state == ParserState.inside_transaction:
            start, end = split_parameters.group(1, 2)
            if start == end:
                state = ParserState.new_transaction

        row["Memo"] = SPLIT_RE.sub("", memo)
        row["Transaction ID"] = num
        rename_row(row, "Payee", "Description")
        rename_row(row, "Account", "Transfer Account")
        rename_row(row, "Category Group/Category", "Account")
        rename_row(row, "Outflow", "Deposit")
        rename_row(row, "Inflow", "Withdrawal")

        del row["Flag"]
        del row["Cleared"]
        del row["Category Group"]
        del row["Category"]
        yield row

def add_source_transactions(rows):
    transaction_sums = collections.defaultdict(
        lambda: {
            "Withdrawal": 0,
            "Deposit": 0,
            "Memo": "Haben",
        }
    )
    for row in rows:
        tx_id = row["Transaction ID"]
        tx = transaction_sums[tx_id]
        withdrawal = int(row["Deposit"]) - int(row["Withdrawal"])
        tx["Withdrawal"] += withdrawal
        tx["Date"] = row["Date"]
        tx["Account"] = row["Transfer Account"]
        tx["Transaction ID"] = tx_id
        tx["Description"] = row["Description"]

    for tx in transaction_sums.values():
        if tx["Withdrawal"] < 0:
            tx["Deposit"] = -tx["Withdrawal"]
            tx["Withdrawal"] = 0

    return transaction_sums.values()


def write_rows(rows):
    with open(converted_path, "w") as fd:
        writer = csv.DictWriter(fd, rows[0].keys())

        writer.writeheader()
        writer.writerows(rows)


def main():
    converted_rows = list(iter_rows(read_rows()))
    rows = sorted(
        (
            *converted_rows,
            *add_source_transactions(converted_rows),
        ),
        key=lambda r: r["Transaction ID"],
    )
    write_rows(rows)


if __name__ == "__main__":
    main()
