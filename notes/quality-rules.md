Rule 1: Valid sale date
Column:       sale_date
Fails when:   sale_date is NULL
Action:       goes to quarantine
Reason:       the 29 POS rows with an impossible or blank date

Rule 2: Invalid quantity
Column:       quantity
Fails when:   quantity is 0 or negative, or NULL
Action:       goes to quarantine
Reason:       the 23 POS rows with quantity 0 or negative

Rule 3: Invalid unit price
Column:       unit_price_cad
Fails when:   unit_price_cad is 0 or negative, or NULL
Action:       goes to quarantine
Reason:       the 37 POS rows where unit_price is N/A, -1.00 or 0.00

Rule 4: Duplicate rows
Column:       sale_id
Fails when:   two or more rows share the same sale_id
Action:       quarantine all copies
Reason:       the 35 POS rows
