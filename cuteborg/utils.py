def print_table(header, rows):
    col_max_widths = [len(hdr) for hdr in header]

    for row in rows:
        for col, cell in enumerate(row):
            col_max_widths[col] = max(
                len(cell),
                col_max_widths[col],
            )

    fmt = " | ".join(
        "{{:<{w}s}}".format(
            w=width
        )
        for width in col_max_widths,
    )

    print(fmt.format(*("="*w for w in col_max_widths)))
    print(fmt.format(*header))
    print(fmt.format(*("-"*w for w in col_max_widths)))

    for row in rows:
        print(fmt.format(*row))
