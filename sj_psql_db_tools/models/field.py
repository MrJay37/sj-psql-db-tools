class Field:
    _allowed_data_types = {
        'int2',
        'int4',
        'int8',
        'float4',
        'float8',
        'varchar',
        'text',
        'boolean',
        'date',
        'timestamp',
        'timestamptz',
        'json',
        'jsonb',
        'uuid',
        'bytea',
    }

    def __init__(self, name: str, data_type: str = 'varchar', is_nullable: bool = True, default_value=None):
        self.name = name
        self.data_type = data_type
        if self.data_type not in self._allowed_data_types:
            raise ValueError(f"Data type '{self.data_type}' is not supported.")

        self.is_nullable = is_nullable
        self.default_value = default_value

    def __repr__(self):
        return f"Field(name={self.name}, data_type={self.data_type}, is_nullable={self.is_nullable}, default_value={self.default_value})"