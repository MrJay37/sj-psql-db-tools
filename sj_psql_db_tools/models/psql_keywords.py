class PSQLKeyword:
    def __init__(self, keyword: str):
        self.keyword = keyword

    def __str__(self):
        return self.keyword

class PSQLKeywords:
    true = PSQLKeyword("true")

    false = PSQLKeyword("false")

    null = PSQLKeyword("null")

    now = PSQLKeyword("now()")

    gen_random_uuid = PSQLKeyword("gen_random_uuid()")
