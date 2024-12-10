def clean_string(column):
    return f"LTRIM(REGEXP_REPLACE({column}, '[^A-Za-z0-9\s,.''''\";:!?()_&%$#@]', ''))"


def capitalized_case(column):
    return f"REGEXP_REPLACE(LOWER({column}), '\y([a-z])', UPPER('\1'))"


def upper_case(column):
    return f"UPPER({column})"


def marital_status(column):
    return f"""
        CASE WHEN ({column} = 'M')
            THEN 'MARRIED'
        WHEN ({column} = 'S')
            THEN 'SINGLE'
        WHEN ({column} = 'D')
            THEN 'DIVORCE'
        ELSE '(unknown)'
        END
    """


def gender(column):
    return f"""
        CASE WHEN ({column} = 'F')
            THEN 'FEMALE'
        WHEN ({column} = 'M')
            THEN 'MALE'
        ELSE '(unknown)'
        END
    """
