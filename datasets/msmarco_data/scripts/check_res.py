with open('filter_in_memory/result/query_woh/base/original/result_10_idx_uint32.tsv', 'r') as f:
    count = 0
    for i, line in enumerate(f, start=1):  # start=1 to make row numbers human-friendly
        row = line.strip().split('\t')
            
        # Check if all entries are numbers
        try:
            numbers = [float(x) for x in row]
        except ValueError:
            print(f"Row {i} contains non-numeric values.")
            continue
        zero_count = sum(1 for x in numbers if x == 0.0)
        if zero_count > 0:
            count += 1

            print(f"Row {i} does not contain 10 numbers (has {10-zero_count})")
    print(f"There are {count} number of rows not containing 10 numbers")


# Example usage

